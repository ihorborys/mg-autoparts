from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Literal

from dotenv import load_dotenv

load_dotenv()

from .storage import StorageClient
from .price_manager import process_all_prices

app = FastAPI(title="Maxgear API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://127.0.0.1:5173", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

storage = StorageClient()


class ImportAllRequest(BaseModel):
    remote_gz_path: str
    supplier: str  # напр. "AP_GDANSK"


@app.post("/admin/import-all")
def import_all(req: ImportAllRequest):
    try:
        results = process_all_prices(req.supplier, req.remote_gz_path)
        return {"supplier": req.supplier, "results": results}
    except Exception as e:
        raise HTTPException(500, f"Import failed: {e}")
