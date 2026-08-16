import os
import duckdb
import requests
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse
import logging

# ========== CONFIG – ALL FILES IN /tmp ==========
PARQUET_URL = "https://justin-jul17x-04-6fce8e1832ea.herokuapp.com//dl/161209?code=22594d6434a0acda13c97c5320b582441f494f2b909f8f1c"
PARQUET_FILE = "/tmp/users.parquet"          # stored in /tmp
DB_FILE = "/tmp/cache.duckdb"                # persistent DuckDB cache in /tmp

# ========== LOGGING ==========
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ========== FASTAPI APP ==========
app = FastAPI(title="Parquet Search API")

# Global DuckDB connection (initialised lazily)
con = None

def ensure_file_downloaded():
    """Download the Parquet file only if missing (streamed, no RAM explosion)."""
    if os.path.exists(PARQUET_FILE):
        logger.info(f"Parquet file already exists at {PARQUET_FILE}")
        return

    logger.info(f"Downloading {PARQUET_URL} -> {PARQUET_FILE} ...")
    try:
        response = requests.get(PARQUET_URL, stream=True)
        response.raise_for_status()
        total = int(response.headers.get("content-length", 0))
        with open(PARQUET_FILE, "wb") as f:
            downloaded = 0
            for chunk in response.iter_content(chunk_size=1024*1024):  # 1 MB chunks
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total:
                        logger.info(f"Progress: {downloaded/total*100:.1f}%")
        logger.info(f"Download complete. Size: {os.path.getsize(PARQUET_FILE) / 1e9:.2f} GB")
    except Exception as e:
        logger.error(f"Download failed: {e}")
        raise RuntimeError(f"Could not download Parquet file: {e}")

def get_connection():
    """Lazily create a DuckDB connection with a memory limit and a persistent database."""
    global con
    if con is None:
        ensure_file_downloaded()          # download if missing

        # Use a persistent file inside /tmp to avoid keeping everything in RAM
        con = duckdb.connect(database=DB_FILE)
        # Set a safe memory limit (e.g., 400 MB) – adjust if you have more RAM
        con.execute("PRAGMA memory_limit='400MB'")
        con.execute("PRAGMA threads=2")    # reduce concurrency to save memory
        logger.info(f"DuckDB connection established with memory limit. DB file: {DB_FILE}")
    return con

@app.on_event("shutdown")
def shutdown_event():
    global con
    if con:
        con.close()
        logger.info("DuckDB connection closed.")

# ========== ENDPOINTS ==========
@app.get("/health")
def health():
    return {
        "status": "ok",
        "file_exists": os.path.exists(PARQUET_FILE),
        "file_size_gb": round(os.path.getsize(PARQUET_FILE) / 1e9, 2) if os.path.exists(PARQUET_FILE) else 0
    }

@app.get("/search")
def search_mobile(mobile: str = Query(..., description="Mobile number to search")):
    try:
        db = get_connection()   # triggers download if needed

        # Query – DuckDB reads the Parquet file row‑by‑row, never loads it all
        query = """
            SELECT *
            FROM read_parquet(?)
            WHERE mobile = ?
        """
        result = db.execute(query, [PARQUET_FILE, mobile]).fetchall()
        columns = [desc[0] for desc in db.description] if result else []

        if not result:
            return JSONResponse(
                status_code=404,
                content={"found": False, "message": "No record found for this mobile number"}
            )

        rows = [dict(zip(columns, row)) for row in result]
        return {"found": True, "count": len(rows), "data": rows}

    except Exception as e:
        logger.error(f"Search error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
def root():
    return {"message": "Use /search?mobile=XXXXXXXXXX"}

# ========== FOR LOCAL TESTING ==========
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
