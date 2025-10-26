import os
import re
import gzip
import shutil
import yaml
from ftplib import FTP
from datetime import datetime
from pathlib import Path
from typing import Tuple, List, Dict, Any, Optional

import pandas as pd

from .storage import StorageClient


# ----------------------- FTP / unzip -----------------------

def download_file_from_ftp(remote_path: str, local_path: Path) -> None:
    host = os.getenv("FTP_HOST")
    user = os.getenv("FTP_USER")
    pwd = os.getenv("FTP_PASS")
    if not all([host, user, pwd]):
        raise RuntimeError("FTP credentials are missing in .env")
    with FTP(host) as ftp:
        ftp.login(user, pwd)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        with open(local_path, "wb") as f:
            ftp.retrbinary(f"RETR " + remote_path, f.write)


def unzip_gz_file(gz_file: Path, output_csv: Path) -> None:
    with gzip.open(gz_file, "rb") as f_in, open(output_csv, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)


# ----------------------- Config helpers -----------------------

def _config_dir() -> Path:
    # backend/app/price_processor.py -> backend/config/...
    return Path(__file__).resolve().parent.parent / "config"


def _load_supplier_cfg(supplier_name: str) -> dict:
    """Завантажує секцію постачальника з config/suppliers.yaml."""
    cfg_path = _config_dir() / "suppliers.yaml"
    if not cfg_path.exists():
        return {}
    with open(cfg_path, "r", encoding="utf-8") as f:
        all_suppliers = yaml.safe_load(f) or {}
    return (
            all_suppliers.get(supplier_name)
            or all_suppliers.get(supplier_name.upper())
            or all_suppliers.get(supplier_name.lower())
            or {}
    )


# ----------------------- Normalize & parse -----------------------

def _normalize_line_with_cfg(line: str, gt5_to: Optional[int]) -> str:
    """
    Нормалізація рядка:
      - замінюємо '> 5' на gt5_to (або 10, якщо не задано),
      - злипання першого пробілу між буквено-цифровими блоками,
      - інші пробіли замінюємо на ';'.
    """
    repl = str(gt5_to if gt5_to is not None else 10)
    line = re.sub(r">\s*5", repl, line)
    m = re.search(r"\w\s\w*\s\w", line)
    if m:
        line = re.sub(r"\s", "", line, count=1)
    line = re.sub(r"\s", ";", line)
    return line


def raw_csv_to_rows(
        input_csv: Path,
        stock_index: Optional[int] = None,
        stock_header_token: str = "STAN",
        gt5_to: Optional[int] = None,
        skip_rows: int = 0,
) -> List[List[str]]:
    """
    Читає сирий CSV, застосовує normalize, відбирає рядки за stock.
    """
    rows: List[List[str]] = []
    with open(input_csv, "r", encoding="utf-8", errors="ignore") as f:
        for i, raw in enumerate(f):
            if i < skip_rows:
                continue  # пропускаємо службові рядки/шапку
            raw = raw.strip()
            if not raw:
                continue
            try:
                norm = _normalize_line_with_cfg(raw, gt5_to=gt5_to)
                parts = norm.split(";")
                idx = stock_index if stock_index is not None else (len(parts) - 1)
                last = parts[idx] if 0 <= idx < len(parts) else ""
                # лишаємо рядки, де stock — цифра > 0, або це рядок-шапка колонки stock
                if last == stock_header_token or (last.isdigit() and int(last) > 0):
                    rows.append(parts)
            except Exception:
                continue
    return rows


def _rows_to_standard_df(rows: List[List[str]], colmap: Dict[str, int]) -> pd.DataFrame:
    """
    Приводимо сирі рядки до стандартної моделі колонок:
    code, unicode, brand, name, stock, price
    colmap — індекси сирих колонок (0-based).
    """

    def col(name: str) -> pd.Series:
        idx = colmap.get(name)
        if idx is None:
            return pd.Series([None] * len(rows))
        return pd.Series([r[idx] if idx < len(r) else None for r in rows])

    df = pd.DataFrame({
        "code": col("code"),
        "unicode": col("unicode"),
        "brand": col("brand"),
        "name": col("name"),
        "stock": col("stock"),
        "price": col("price"),
    })

    # нормалізація значень
    df["price"] = df["price"].astype(str).str.replace(",", ".", regex=False)
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["stock"] = pd.to_numeric(df["stock"], errors="coerce").fillna(0).astype(int)
    return df


# ----------------------- Pricing & build output -----------------------

def _apply_pricing(
        df: pd.DataFrame,
        factor: float,
        currency_out: str,
        rate: float,
        rounding: Dict[str, int],
) -> pd.Series:
    """
    Обчислює фінальну ціну:
      EUR: price * factor (round EUR digits)
      UAH: price * factor * rate (round UAH digits)
    """
    base = df["price"].fillna(0.0)
    if currency_out.upper() == "UAH":
        val = base * float(factor) * float(rate)
        digits = int(rounding.get("UAH", 0))
    else:
        val = base * float(factor)
        digits = int(rounding.get("EUR", 2))
    return val.round(digits)


def _build_output_df(
        df_std: pd.DataFrame,
        price_final: pd.Series,
        columns_cfg: List[Dict[str, str]],
        supplier_id: Optional[int],
) -> pd.DataFrame:
    """
    Збирає вихідний DataFrame у потрібному порядку і з правильними шапками.
    columns_cfg елементи виду:
      { from: code|unicode|brand|name|stock|price|supplier_id, header: "..." }
    """
    temp = df_std.copy()
    temp["supplier_id"] = supplier_id if supplier_id is not None else None
    temp["price"] = price_final  # універсальне джерело для price_EUR/price_UAH у columns_cfg

    out_cols: Dict[str, pd.Series] = {}
    for col in columns_cfg:
        src = col["from"]
        hdr = col["header"]
        if src not in temp.columns:
            temp[src] = temp.get(src, None)
        out_cols[hdr] = temp[src]

    return pd.DataFrame(out_cols)


# ----------------------- Main pipeline -----------------------

def process_one_price(
        remote_gz_path: str,
        supplier: str,
        supplier_id: Optional[int],
        factor: float,
        currency_out: str,  # "EUR" | "UAH"
        format_: str,  # "xlsx" | "csv"
        rounding: Dict[str, int],  # {"EUR":2, "UAH":0}
        r2_prefix: str,  # ".../{supplier}/"
        columns: List[Dict[str, str]],
        csv_cfg: Optional[Dict[str, Any]] = None,
        rate: float = 1.0,
) -> Tuple[str, str]:
    """
    Повний цикл:
      FTP → unzip → normalize (per suppliers.yaml) → calc → export (xlsx/csv) → upload R2 (+cleanup) → cleanup tmp
    Повертає (key, url) у R2.
    """
    tmp_dir = Path("data/tmp")
    tmp_dir.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now().strftime("%Y%m%d_%H%M")
    supplier_code = supplier.lower()

    gz_path = tmp_dir / f"{supplier_code}_{stamp}.csv.gz"
    csv_path = tmp_dir / f"{supplier_code}_{stamp}.csv"

    # 1) FTP
    download_file_from_ftp(remote_gz_path, gz_path)

    # 2) unzip
    unzip_gz_file(gz_path, csv_path)

    # 3) normalize → standard df (з урахуванням suppliers.yaml)
    sup_cfg = _load_supplier_cfg(supplier)
    layout = sup_cfg.get("raw_layout", {}) or {}
    colmap: Dict[str, int] = (layout.get("columns") or {})
    stock_index = layout.get("stock_index")
    stock_header_token = layout.get("stock_header_token", "STAN")
    gt5_to = layout.get("gt5_to")
    skip_rows = (sup_cfg.get("preprocess") or {}).get("skip_rows", 0)

    rows = raw_csv_to_rows(
        csv_path,
        stock_index=stock_index,
        stock_header_token=stock_header_token,
        gt5_to=gt5_to,
        skip_rows=skip_rows,
    )
    df_std = _rows_to_standard_df(rows, colmap)

    # дублювання (як для AP_GDANSK: unicode=code, name=brand)
    if colmap.get("unicode") == colmap.get("code"):
        df_std["unicode"] = df_std["code"]
    if colmap.get("name") == colmap.get("brand"):
        df_std["name"] = df_std["brand"]

    # 4) розрахунок ціни + округлення
    price_final = _apply_pricing(
        df_std, factor=factor, currency_out=currency_out, rate=rate, rounding=rounding
    )

    # 5) складання вихідного фрейму під columns (із профілю)
    out_df = _build_output_df(
        df_std, price_final, columns_cfg=columns, supplier_id=supplier_id
    )

    # 6) експорт (xlsx / csv)
    ext = "xlsx" if format_.lower() == "xlsx" else "csv"
    out_path = tmp_dir / f"{supplier_code}_{stamp}.{ext}"

    if ext == "xlsx":
        # Без стилів/рамок — “чистий” Excel
        out_df.to_excel(out_path, index=False, engine="xlsxwriter")
        content_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    else:
        delim = (csv_cfg or {}).get("delimiter", ";")
        header = bool((csv_cfg or {}).get("header", True))
        out_df.to_csv(out_path, index=False, sep=delim, header=header, encoding="utf-8")
        content_type = "text/csv"

    # 7) upload → R2 (та локальне очищення за префіксом)
    storage = StorageClient()
    prefix = r2_prefix  # уже з підставленим {supplier}
    key = f"{prefix}{supplier_code}_{stamp}.{ext}"

    # вибираємо keep_last із .env залежно від префікса
    keep_last = 7
    if prefix.startswith("1_23/"):
        keep_last = int(os.getenv("R2_KEEP_123", "7"))
    elif prefix.startswith("1_27/"):
        keep_last = int(os.getenv("R2_KEEP_127", "7"))
    elif prefix.startswith("1_33/site/"):
        keep_last = int(os.getenv("R2_KEEP_133_SITE", "14"))
    elif prefix.startswith("1_33/exist/"):
        keep_last = int(os.getenv("R2_KEEP_133_EXIST", "14"))
    elif prefix.startswith("netto/"):
        keep_last = int(os.getenv("R2_KEEP_NETTO", "5"))

    url = storage.upload_file(
        local_path=str(out_path),
        key=key,
        content_type=content_type,
        cleanup_prefix=prefix,
        keep_last=keep_last,
    )

    # 8) локальний cleanup tmp
    try:
        gz_path.unlink(missing_ok=True)
        csv_path.unlink(missing_ok=True)
        out_path.unlink(missing_ok=True)
    except Exception:
        pass

    return key, url
