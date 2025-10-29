import os
import re
import gzip
import shutil
import yaml
import ftplib
from datetime import datetime
from typing import Tuple, List, Dict, Any, Optional

import pandas as pd
from pathlib import Path

from .paths import TEMP_DIR
from .storage import StorageClient


# ----------------------- FTP / unzip -----------------------
def download_file_from_ftp(remote_path: str, local_path: Path) -> None:
    host = os.getenv("FTP_HOST")
    user = os.getenv("FTP_USER")
    pwd = os.getenv("FTP_PASS")
    if not all([host, user, pwd]):
        raise RuntimeError("FTP credentials are missing in .env")

    # допоміжний виконавець
    def _retr(ftp):
        ftp.set_pasv(True)  # як у FileZilla (PASV)
        ftp.login(user, pwd)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        with open(local_path, "wb") as f:
            ftp.retrbinary(f"RETR " + remote_path, f.write)
        ftp.quit()

    # 1) спроба через Explicit TLS (FTPS)
    try:
        ftps = ftplib.FTP_TLS(host, timeout=20)
        ftps.auth()  # AUTH TLS
        ftps.prot_p()  # шифрувати data channel
        _retr(ftps)
        return
    except ftplib.all_errors as e_tls:
        # 2) якщо TLS не доступний — пробуємо звичайний FTP
        try:
            ftp = ftplib.FTP(host, timeout=20)
            _retr(ftp)
            return
        except ftplib.all_errors as e_plain:
            # показати, що пробували обидва варіанти
            raise RuntimeError(f"FTP/FTPS failed. FTPS: {e_tls}; FTP: {e_plain}")


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
    Нормалізація рядка для «пробільних» форматів:
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
        *,
        stock_index: Optional[int],
        stock_header_token: str = "STAN",
        gt5_to: Optional[int] = None,
        skip_rows: int = 0,
        normalize_mode: str = "spaces",  # "spaces" | "csv"
) -> List[List[str]]:
    """
    Читає сирий CSV і повертає рядки (list[str]) відфільтровані за наявністю (stock>0).
    - normalize_mode="spaces": застосовуємо _normalize_line_with_cfg (для файлів із пробільними розділювачами)
    - normalize_mode="csv":      НЕ чіпаємо пробіли; рядок уже має ; як розділювач (Motorol)
    """
    rows: List[List[str]] = []
    with open(input_csv, "r", encoding="utf-8", errors="ignore") as f:
        for i, raw in enumerate(f):
            if i < skip_rows:
                continue
            raw = raw.strip()
            if not raw:
                continue

            if normalize_mode == "csv":
                parts = raw.split(";")
            else:
                norm = _normalize_line_with_cfg(raw, gt5_to=gt5_to)
                parts = norm.split(";")

            if not parts:
                continue

            idx = stock_index if stock_index is not None else (len(parts) - 1)
            if idx < 0 or idx >= len(parts):
                continue

            val = (parts[idx] or "").strip()

            # пропускаємо службовий заголовок стоку
            if val.lower() == (stock_header_token or "").lower():
                continue

            # нормалізуємо '>5' у числове значення
            if gt5_to is not None and (val.startswith(">") or val.replace(" ", "").startswith(">")):
                val = str(gt5_to)
                parts[idx] = val

            try:
                if int(val) <= 0:
                    continue
            except ValueError:
                continue

            rows.append(parts)
    return rows


def _rows_to_standard_df(rows: List[List[str]], colmap: Dict[str, int]) -> pd.DataFrame:
    """
    Приводимо сирі рядки до стандартної моделі колонок:
    code, unicode, brand, name, stock, price
    colmap — індекси сирих колонок (0-based).
    """

    def take(r: List[str], idx: Optional[int]) -> str:
        if idx is None or idx < 0 or idx >= len(r):
            return ""
        return (r[idx] or "").strip()

    data: List[List[Any]] = []
    for r in rows:
        code = take(r, colmap.get("code"))
        unicode_ = take(r, colmap.get("unicode")) or code
        brand = take(r, colmap.get("brand"))
        name = take(r, colmap.get("name")) or brand
        stock_s = take(r, colmap.get("stock"))
        price_s = take(r, colmap.get("price"))

        # stock -> int
        try:
            stock = int(stock_s)
        except Exception:
            stock = 0

        # price -> float (коми/зайві символи прибираємо)
        ps = price_s.replace(",", ".")
        ps = re.sub(r"[^0-9.]", "", ps)
        try:
            price = float(ps)
        except Exception:
            price = float("nan")

        data.append([code, unicode_, brand, name, stock, price])

    df = pd.DataFrame(data, columns=["code", "unicode", "brand", "name", "stock", "price"])
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
    base = pd.to_numeric(df["price"], errors="coerce").fillna(0.0).astype(float)
    if currency_out.upper() == "UAH":
        val = base * float(factor) * float(rate)
        digits = int(rounding.get("UAH", 0))
    else:
        val = base * float(factor)
        digits = int(rounding.get("EUR", 2))
    return val.round(digits).astype(float)


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
    temp["price"] = price_final  # універсальне джерело для price_EUR/price_UAH

    out_cols: Dict[str, pd.Series] = {}
    for col in columns_cfg:
        src = col["from"]
        hdr = col["header"]
        if src not in temp.columns:
            temp[src] = temp.get(src, None)
        out_cols[hdr] = temp[src]

    return pd.DataFrame(out_cols)


# ----------------------- Materialize to CSV -----------------------

def _materialize_to_csv(remote_path: str, tmp_dir: Path) -> tuple[Path, list[Path]]:
    """
    Приводить будь-яке джерело до локального CSV.
    Підтримує:
      - локальний .csv → повертає як є
      - локальний .gz  → розпаковує у tmp
      - ftp-шлях (.gz) → качає у tmp → розпаковує у tmp
    Повертає: (csv_path, cleanup_paths)
    """
    cleanup: list[Path] = []

    if os.path.exists(remote_path):
        p = Path(remote_path)
        if p.suffix.lower() == ".csv":
            return p, cleanup
        if p.suffix.lower() == ".gz":
            csv_out = tmp_dir / f"{p.stem}"
            if csv_out.suffix.lower() != ".csv":
                csv_out = csv_out.with_suffix(".csv")
            unzip_gz_file(p, csv_out)
            cleanup.append(csv_out)
            return csv_out, cleanup
        raise ValueError(f"Unsupported local file type: {p.suffix}")
    else:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        gz_tmp = tmp_dir / f"ftp_{stamp}.csv.gz"
        csv_tmp = tmp_dir / f"ftp_{stamp}.csv"
        download_file_from_ftp(remote_path, gz_tmp)
        unzip_gz_file(gz_tmp, csv_tmp)
        cleanup.extend([gz_tmp, csv_tmp])
        return csv_tmp, cleanup


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
        delete_input_after: bool = False,  # якщо локальний вхід (пошта) — видалити після обробки
) -> Tuple[str, str]:
    """
    Повний цикл:
      materialize (лок. CSV або FTP+GZ) → normalize (per suppliers.yaml) → calc → export (xlsx/csv)
      → upload R2 (+cleanup cloud) → cleanup tmp/local
    Повертає (key, url) у R2.
    """
    tmp_dir = TEMP_DIR
    tmp_dir.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now().strftime("%Y%m%d_%H%M")
    supplier_code = supplier.lower()

    # 0) завжди отримуємо локальний CSV + список тимчасових файлів
    csv_path, cleanup_paths = _materialize_to_csv(remote_gz_path, tmp_dir)

    # 1) normalize → standard df
    sup_cfg = _load_supplier_cfg(supplier)
    layout = sup_cfg.get("raw_layout", {}) or {}
    colmap: Dict[str, int] = (layout.get("columns") or {})
    stock_index = layout.get("stock_index")
    stock_header_token = layout.get("stock_header_token", "STAN")
    gt5_to = layout.get("gt5_to")
    skip_rows = (sup_cfg.get("preprocess") or {}).get("skip_rows", 0)
    normalize_mode = (sup_cfg.get("normalize") or {}).get("mode", "spaces")

    rows = raw_csv_to_rows(
        csv_path,
        stock_index=stock_index,
        stock_header_token=stock_header_token,
        gt5_to=gt5_to,
        skip_rows=skip_rows,
        normalize_mode=normalize_mode,
    )

    df_std = _rows_to_standard_df(rows, colmap)

    # дублювання (як для AP_GDANSK: unicode=code, name=brand)
    if colmap.get("unicode") == colmap.get("code"):
        df_std["unicode"] = df_std["code"]
    if colmap.get("name") == colmap.get("brand"):
        df_std["name"] = df_std["brand"]

    # 2) calc
    price_final = _apply_pricing(
        df_std, factor=factor, currency_out=currency_out, rate=rate, rounding=rounding
    )

    # 3) build output
    out_df = _build_output_df(
        df_std, price_final, columns_cfg=columns, supplier_id=supplier_id
    )

    # 4) export
    ext = "xlsx" if format_.lower() == "xlsx" else "csv"
    out_path = tmp_dir / f"{supplier_code}_{stamp}.{ext}"

    if ext == "xlsx":
        out_df.to_excel(out_path, index=False, engine="xlsxwriter")
        content_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    else:
        delim = (csv_cfg or {}).get("delimiter", ";")
        header = bool((csv_cfg or {}).get("header", True))
        out_df.to_csv(out_path, index=False, sep=delim, header=header, encoding="utf-8")
        content_type = "text/csv"

    # 5) upload + cloud cleanup policy
    storage = StorageClient()
    prefix = r2_prefix
    key = f"{prefix}{supplier_code}_{stamp}.{ext}"

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

    # 6) local cleanup
    try:
        # прибираємо згенерований вихідний файл
        out_path.unlink(missing_ok=True)
        # прибираємо тимчасові (те, що повернув хелпер)
        for p in cleanup_paths:
            p.unlink(missing_ok=True)
        # якщо вхід був локальний і хочемо чистити — видаляємо й його
        if delete_input_after and os.path.exists(remote_gz_path):
            rp = Path(remote_gz_path)
            if rp.exists() and rp.resolve() not in [out_path.resolve(), *[c.resolve() for c in cleanup_paths]]:
                rp.unlink(missing_ok=True)
    except Exception:
        pass

    return key, url
