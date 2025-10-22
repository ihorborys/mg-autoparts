from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Literal
from dotenv import load_dotenv
import os

from .storage import StorageClient
from .price_processor import process_one_price  # <- додали

# 1) .env
load_dotenv()

# 2) FastAPI
app = FastAPI(title="Maxgear API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://127.0.0.1:5173", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 3) R2 client
storage = StorageClient()


# ---- models ----
class ImportOneRequest(BaseModel):
    remote_gz_path: str  # напр. "27958_ce.gz"
    supplier: str = "gdansk"
    factor: Literal["1_27", "1_23"] = "1_27"


# ---- routes ----
@app.get("/")
def read_root():
    return {"message": "API сервер для AutoParts працює!!"}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/prices/latest")
def prices_latest():
    p123 = os.getenv("R2_PREFIX_123", "1_23/")
    p127 = os.getenv("R2_PREFIX_127", "1_27/")
    b2b_key = storage.latest_key(p123)
    site_key = storage.latest_key(p127)
    return {
        "b2b_key": b2b_key,
        "b2b_url": storage.url_for(b2b_key),
        "site_key": site_key,
        "site_url": storage.url_for(site_key),
    }


@app.post("/admin/import-one")
def import_one(req: ImportOneRequest):
    if req.factor not in ("1_27", "1_23"):
        raise HTTPException(400, "factor must be '1_27' or '1_23'")
    key, url = process_one_price(
        remote_gz_path=req.remote_gz_path,
        supplier=req.supplier,
        factor=req.factor,  # type: ignore
    )
    return {"key": key, "url": url, "factor": req.factor, "supplier": req.supplier}
