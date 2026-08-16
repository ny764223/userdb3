import os
import duckdb
import requests
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse
import logging
import time

# ========== CONFIG ==========
PARQUET_URL = "https://filetolink2bot-0944e29c8e8c.herokuapp.com/dl/6a81b7429cdecd60f30f3b52"
PARQUET_FILE = "/tmp/users.parquet"
DB_FILE = "/tmp/cache.duckdb"

# Download settings – conservative to avoid server overload
DOWNLOAD_TIMEOUT = 120          # seconds per read attempt
MAX_RETRIES = 5                 # retries for the whole file
RETRY_DELAY = 10                # seconds between retries

# ========== LOGGING ==========
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ========== FASTAPI APP ==========
app = FastAPI(title="Parquet Search API")
con = None

# ========== ROBUST SINGLE‑THREAD DOWNLOAD ==========
def download_file_single(url, dest):
    """Download a file with retries, streaming, and progress logging."""
    if os.path.exists(dest):
        # Check if existing file is complete (optional: compare size if we knew it)
        logger.info(f"File already exists: {dest}")
        return

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(f"Download attempt {attempt}/{MAX_RETRIES}: {url} -> {dest}")
            headers = {"User-Agent": "Mozilla/5.0"}
            with requests.get(url, headers=headers, stream=True, timeout=DOWNLOAD_TIMEOUT) as r:
                r.raise_for_status()
                total_size = int(r.headers.get("Content-Length", 0))
                downloaded = 0
                with open(dest, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024*1024):  # 1 MB
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            if total_size:
                                logger.info(f"Progress: {downloaded / total_size * 100:.1f}%")
                # verify size
                if total_size and downloaded != total_size:
                    raise RuntimeError(f"Size mismatch: {downloaded}/{total_size} bytes")
                logger.info(f"Download complete: {dest} ({downloaded / 1e9:.2f} GB)")
                return  # success
        except Exception as e:
            logger.error(f"Attempt {attempt} failed: {e}")
            if attempt < MAX_RETRIES:
                wait = RETRY_DELAY * attempt
                logger.info(f"Retrying in {wait} seconds...")
                time.sleep(wait)
            else:
                raise RuntimeError(f"All {MAX_RETRIES} attempts failed: {e}")

def ensure_file_downloaded():
    if not os.path.exists(PARQUET_FILE):
        download_file_single(PARQUET_URL, PARQUET_FILE)

def get_connection():
    global con
    if con is None:
        ensure_file_downloaded()
        con = duckdb.connect(database=DB_FILE)
        con.execute("PRAGMA memory_limit='400MB'")
        con.execute("PRAGMA threads=2")
        logger.info("DuckDB connection established.")
    return con

@app.on_event("shutdown")
def shutdown_event():
    global con
    if con:
        con.close()

# ========== ENDPOINTS ==========
@app.get("/health")
def health():
    exists = os.path.exists(PARQUET_FILE)
    size = os.path.getsize(PARQUET_FILE) if exists else 0
    return {"status": "ok", "file_exists": exists, "file_size_gb": round(size / 1e9, 2)}

@app.get("/search")
def search_mobile(mobile: str = Query(..., description="Mobile number to search")):
    try:
        db = get_connection()
        query = """
            SELECT
                mobile,
                name,
                fname,
                address,
                doc_id,
                circle,
                email,
                alt
            FROM read_parquet(?)
            WHERE mobile = ?
            LIMIT 20
        """
        result = db.execute(query, [PARQUET_FILE, mobile]).fetchall()
        columns = ["mobile", "name", "fname", "address", "doc_id", "circle", "email", "alt"]
        if not result:
            return JSONResponse(status_code=404, content={"found": False, "message": "No record found"})
        rows = [dict(zip(columns, row)) for row in result]
        return {"found": True, "count": len(rows), "data": rows}
    except Exception as e:
        logger.error(f"Search error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
def root():
    return {"message": "Use /search?mobile=XXXXXXXXXX"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
