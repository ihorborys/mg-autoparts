import os, re, csv, gzip, shutil
from ftplib import FTP
from datetime import datetime
from pathlib import Path
from typing import Literal, Tuple
import pandas as pd

from .storage import StorageClient

Factor = Literal["1_23", "1_27"]


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
            ftp.retrbinary(f"RETR {remote_path}", f.write)


def unzip_gz_file(gz_file: Path, output_csv: Path) -> None:
    with gzip.open(gz_file, "rb") as f_in, open(output_csv, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)


def _normalize_line(line: str) -> str:
    """
    Повторює твою поточну логику: заміна '> 5' на '10', злипання першого пробілу,
    потім заміна пробілів на ';' (щоб стало схоже на CSV по ';').
    """
    line = re.sub(r"> 5", "10", line)
    m = re.search(r"\w\s\w*\s\w", line)
    if m:
        line = re.sub(r"\s", "", line, count=1)
    line = re.sub(r"\s", ";", line)
    return line


def raw_csv_to_rows(input_csv: Path) -> list[list[str]]:
    rows: list[list[str]] = []
    # читаємо як сирий текст, бо формат специфічний
    with open(input_csv, "r", encoding="utf-8", errors="ignore") as f:
        for raw in f:
            raw = raw.strip()
            if not raw:
                continue
            try:
                # фільтр по останній “сток” колонці
                parts = raw.split(";")
                last = parts[-1] if parts else ""
                if last == "STAN" or last == "> 5" or (last.isdigit() and int(last) > 0):
                    norm = _normalize_line(raw)
                    rows.append(norm.split(";"))
            except Exception:
                # просто пропускаємо некоректний рядок, як у тебе
                continue
    return rows


def make_excel(rows: list[list[str]], output_xlsx: Path) -> None:
    # Твої rows зараз без шапки. Додаємо шапку під нашу узгоджену схему:
    # code; unicode; brand; name; stock; price_EUR
    df = pd.DataFrame(rows, columns=None)
    # якщо сирі рядки вже мають саме такий порядок — ок.
    # якщо ні — поки що залишаємо без заголовків, але додаємо "header" вручну:
    output_xlsx.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(output_xlsx, index=False, header=False, engine="xlsxwriter")


def process_one_price(
        remote_gz_path: str,
        supplier: str,
        factor: Factor,  # "1_27" (сайт) або "1_23" (опт)
) -> Tuple[str, str]:
    """
    1) тягне .gz з FTP
    2) розпаковує до CSV
    3) робить XLSX (як у твоєму форматі)
    4) вантажить у R2 за фактором:
        - 1_27 → префікс site_*.csv/.xlsx (тут поки заливаємо XLSX як proof)
        - 1_23 → префікс b2b_*.xlsx
    Повертає (key, url) з R2.
    """
    tmp_dir = Path("data/tmp")
    tmp_dir.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now().strftime("%Y%m%d_%H%M")
    gz_path = tmp_dir / f"{supplier}_{stamp}.csv.gz"
    csv_path = tmp_dir / f"{supplier}_{stamp}.csv"
    xlsx_path = tmp_dir / f"{supplier}_{stamp}.xlsx"

    # 1) FTP
    download_file_from_ftp(remote_gz_path, gz_path)

    # 2) unzip
    unzip_gz_file(gz_path, csv_path)

    # 3) форматування → xlsx (повторюємо твою логіку)
    rows = raw_csv_to_rows(csv_path)
    make_excel(rows, xlsx_path)

    # 4) upload → R2
    storage = StorageClient()
    supplier_code = supplier.lower()

    if factor == "1_27":
        prefix_tmpl = os.getenv("R2_PREFIX_127", "1_27/{supplier}/")
        keep = int(os.getenv("R2_KEEP_127", "7"))
    else:
        prefix_tmpl = os.getenv("R2_PREFIX_123", "1_23/{supplier}/")
        keep = int(os.getenv("R2_KEEP_123", "7"))

    prefix = prefix_tmpl.format(supplier=supplier_code)
    key = f"{prefix}{supplier_code}_{stamp}.xlsx"

    url = storage.upload_file(
        local_path=str(xlsx_path),
        key=key,
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        cleanup_prefix=prefix,  # ← тут вказуємо папку для очищення
        keep_last=keep,  # ← скільки залишити
    )

    # 5) прибирання (тимчасові файли)
    try:
        gz_path.unlink(missing_ok=True)
        csv_path.unlink(missing_ok=True)
        xlsx_path.unlink(missing_ok=True)
    except Exception:
        pass

    return key, url
