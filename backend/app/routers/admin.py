from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

# Імпортуємо функцію обробки з сусідньої папки (тому дві крапки ..)
from ..price_manager import process_all_prices

# Створюємо роутер замість цілого додатку FastAPI
router = APIRouter()

# Модель для вхідних даних (перенесли сюди)
class ImportAllRequest(BaseModel):
    remote_gz_path: str
    supplier: str  # напр. "AP_GDANSK"

# Визначаємо маршрут.
# Зверніть увагу: ми пишемо просто "/import-all", а не "/admin/import-all".
# Префікс "/admin" ми додамо в головному файлі main.py.
@router.post("/import-all")
def import_all(req: ImportAllRequest):
    try:
        print(f"[INFO] Admin received import request for: {req.supplier}")
        # Викликаємо функцію, яка запустить обробку
        results = process_all_prices(req.supplier, req.remote_gz_path)
        return {"supplier": req.supplier, "results": results}
    except Exception as e:
        print(f"[ERROR] Import failed: {e}")
        raise HTTPException(status_code=500, detail=f"Import failed: {str(e)}")