from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import os

from .storage import StorageClient

load_dotenv()

app = FastAPI(title="Maxgear API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=['http://127.0.0.1:5173', 'http://localhost:5173'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

storage = StorageClient()


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
