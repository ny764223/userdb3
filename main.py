import os
import duckdb
import requests
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# ========== CONFIG ==========
PARQUET_URL = "https://filetolink2bot-0944e29c8e8c.herokuapp.com/dl/6a81b7429cdecd60f30f3b52"
PARQUET_FILE = "/tmp/users.parquet"
DB_FILE = "/tmp/cache.duckdb"

# Parallel download settings
CONNECTIONS = 16
CHUNK_SIZE = 1024 * 1024
MAX_RETRIES = 3

# ========== LOGGING ==========
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ========== FASTAPI APP ==========
app = FastAPI(title="Parquet Search API")
con = None

# ========== PARALLEL DOWNLOAD ==========
def download_chunk(url, tmp_path, start, end, retries=MAX_RETRIES):
    headers = {"User-Agent": "Mozilla/5.0", "Range": f"bytes={start}-{end}"}
    expected_len = end - start + 1
    for attempt in range(1, retries + 1):
        try:
            with requests.get(url, headers=headers, stream=True, timeout=30) as r:
                r.raise_for_status()
                with open(tmp_path, "r+b") as f:
                    f.seek(start)
                    written = 0
                    for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                        if chunk:
                            f.write(chunk)
                            written += len(chunk)
                    if written == expected_len:
                        return
                    else:
                        raise RuntimeError(f"Partial write: {written}/{expected_len}")
        except Exception as e:
            logger.warning(f"Chunk {start}-{end} attempt {attempt} failed: {e}")
            time.sleep(2 * attempt)
    raise RuntimeError(f"Failed chunk {start}-{end} after {retries} attempts")

def download_file_parallel(url, dest):
    head = requests.head(url, headers={"User-Agent": "Mozilla/5.0"})
    head.raise_for_status()
    total_size = int(head.headers.get("Content-Length", 0))
    accepts_ranges = head.headers.get("Accept-Ranges", "").lower() == "bytes"

    if os.path.exists(dest) and os.path.getsize(dest) == total_size:
        logger.info(f"File already exists and is complete: {dest}")
        return

    logger.info(f"Downloading {url} -> {dest} ({total_size / 1e9:.2f} GB) with {CONNECTIONS} connections...")
    tmp_path = dest + ".tmp"
    with open(tmp_path, "wb") as f:
        f.truncate(total_size)

    if accepts_ranges and total_size > 0:
        chunk_size = total_size // CONNECTIONS
        futures = []
        with ThreadPoolExecutor(max_workers=CONNECTIONS) as executor:
            for i in range(CONNECTIONS):
                start = i * chunk_size
                end = total_size - 1 if i == CONNECTIONS - 1 else (start + chunk_size - 1)
                futures.append(executor.submit(download_chunk, url, tmp_path, start, end))
            for f in as_completed(futures):
                f.result()
    else:
        logger.warning("Server does not support Range, falling back to single-thread download.")
        with requests.get(url, stream=True, timeout=30) as r:
            r.raise_for_status()
            with open(tmp_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                    if chunk:
                        f.write(chunk)

    downloaded = os.path.getsize(tmp_path)
    if downloaded == total_size:
        os.rename(tmp_path, dest)
        logger.info(f"Download complete and verified: {dest}")
    else:
        os.remove(tmp_path)
        raise RuntimeError(f"Size mismatch: {downloaded}/{total_size} bytes")

def ensure_file_downloaded():
    if not os.path.exists(PARQUET_FILE):
        download_file_parallel(PARQUET_URL, PARQUET_FILE)

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
        # Your exact query – specific columns, LIMIT 20
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
