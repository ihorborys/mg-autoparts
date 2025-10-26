# import os, re, csv, gzip, shutil
# from ftplib import FTP
# from datetime import datetime
# from pathlib import Path
# from typing import Literal, Tuple
# import pandas as pd
#
# from .storage import StorageClient
#
# Factor = Literal["1_23", "1_27"]
#
#
# def download_file_from_ftp(remote_path: str, local_path: Path) -> None:
#     host = os.getenv("FTP_HOST")
#     user = os.getenv("FTP_USER")
#     pwd = os.getenv("FTP_PASS")
#     if not all([host, user, pwd]):
#         raise RuntimeError("FTP credentials are missing in .env")
#     with FTP(host) as ftp:
#         ftp.login(user, pwd)
#         local_path.parent.mkdir(parents=True, exist_ok=True)
#         with open(local_path, "wb") as f:
#             ftp.retrbinary(f"RETR {remote_path}", f.write)
#
#
# def unzip_gz_file(gz_file: Path, output_csv: Path) -> None:
#     with gzip.open(gz_file, "rb") as f_in, open(output_csv, "wb") as f_out:
#         shutil.copyfileobj(f_in, f_out)
#
#
# def _normalize_line(line: str) -> str:
#     """
#     Повторює твою поточну логику: заміна '> 5' на '10', злипання першого пробілу,
#     потім заміна пробілів на ';' (щоб стало схоже на CSV по ';').
#     """
#     line = re.sub(r"> 5", "10", line)
#     m = re.search(r"\w\s\w*\s\w", line)
#     if m:
#         line = re.sub(r"\s", "", line, count=1)
#     line = re.sub(r"\s", ";", line)
#     return line
#
#
# def raw_csv_to_rows(input_csv: Path) -> list[list[str]]:
#     rows: list[list[str]] = []
#     # читаємо як сирий текст, бо формат специфічний
#     with open(input_csv, "r", encoding="utf-8", errors="ignore") as f:
#         for raw in f:
#             raw = raw.strip()
#             if not raw:
#                 continue
#             try:
#                 # фільтр по останній “сток” колонці
#                 parts = raw.split(";")
#                 last = parts[-1] if parts else ""
#                 if last == "STAN" or last == "> 5" or (last.isdigit() and int(last) > 0):
#                     norm = _normalize_line(raw)
#                     rows.append(norm.split(";"))
#             except Exception:
#                 # просто пропускаємо некоректний рядок, як у тебе
#                 continue
#     return rows
#
#
# def make_excel(rows: list[list[str]], output_xlsx: Path) -> None:
#     # Твої rows зараз без шапки. Додаємо шапку під нашу узгоджену схему:
#     # code; unicode; brand; name; stock; price_EUR
#     df = pd.DataFrame(rows, columns=None)
#     # якщо сирі рядки вже мають саме такий порядок — ок.
#     # якщо ні — поки що залишаємо без заголовків, але додаємо "header" вручну:
#     output_xlsx.parent.mkdir(parents=True, exist_ok=True)
#     df.to_excel(output_xlsx, index=False, header=False, engine="xlsxwriter")
#
#
# def process_one_price(
#         remote_gz_path: str,
#         supplier: str,
#         factor: Factor,  # "1_27" (сайт) або "1_23" (опт)
# ) -> Tuple[str, str]:
#     """
#     1) тягне .gz з FTP
#     2) розпаковує до CSV
#     3) робить XLSX (як у твоєму форматі)
#     4) вантажить у R2 за фактором:
#         - 1_27 → префікс site_*.csv/.xlsx (тут поки заливаємо XLSX як proof)
#         - 1_23 → префікс b2b_*.xlsx
#     Повертає (key, url) з R2.
#     """
#     tmp_dir = Path("data/tmp")
#     tmp_dir.mkdir(parents=True, exist_ok=True)
#
#     stamp = datetime.now().strftime("%Y%m%d_%H%M")
#     gz_path = tmp_dir / f"{supplier}_{stamp}.csv.gz"
#     csv_path = tmp_dir / f"{supplier}_{stamp}.csv"
#     xlsx_path = tmp_dir / f"{supplier}_{stamp}.xlsx"
#
#     # 1) FTP
#     download_file_from_ftp(remote_gz_path, gz_path)
#
#     # 2) unzip
#     unzip_gz_file(gz_path, csv_path)
#
#     # 3) форматування → xlsx (повторюємо твою логіку)
#     rows = raw_csv_to_rows(csv_path)
#     make_excel(rows, xlsx_path)
#
#     # 4) upload → R2
#     storage = StorageClient()
#     supplier_code = supplier.lower()
#
#     if factor == "1_27":
#         prefix_tmpl = os.getenv("R2_PREFIX_127", "1_27/{supplier}/")
#         keep = int(os.getenv("R2_KEEP_127", "7"))
#     else:
#         prefix_tmpl = os.getenv("R2_PREFIX_123", "1_23/{supplier}/")
#         keep = int(os.getenv("R2_KEEP_123", "7"))
#
#     prefix = prefix_tmpl.format(supplier=supplier_code)
#     key = f"{prefix}{supplier_code}_{stamp}.xlsx"
#
#     url = storage.upload_file(
#         local_path=str(xlsx_path),
#         key=key,
#         content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
#         cleanup_prefix=prefix,  # ← тут вказуємо папку для очищення
#         keep_last=keep,  # ← скільки залишити
#     )
#
#     # 5) прибирання (тимчасові файли)
#     try:
#         gz_path.unlink(missing_ok=True)
#         csv_path.unlink(missing_ok=True)
#         xlsx_path.unlink(missing_ok=True)
#     except Exception:
#         pass
#
#     return key, url


import os, re, gzip, shutil
from ftplib import FTP
from datetime import datetime
from pathlib import Path
from typing import Tuple, List, Dict, Any, Optional
import pandas as pd

from .storage import StorageClient


# ---------- FTP / unzip / normalize лишаємо як у тебе ----------
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


def _normalize_line(line: str) -> str:
    line = re.sub(r"> 5", "10", line)
    m = re.search(r"\w\s\w*\s\w", line)
    if m:
        line = re.sub(r"\s", "", line, count=1)
    line = re.sub(r"\s", ";", line)
    return line


def raw_csv_to_rows(input_csv: Path) -> List[List[str]]:
    rows: List[List[str]] = []
    with open(input_csv, "r", encoding="utf-8", errors="ignore") as f:
        for raw in f:
            raw = raw.strip()
            if not raw:
                continue
            try:
                parts = raw.split(";")
                last = parts[-1] if parts else ""
                if last == "STAN" or last == "> 5" or (last.isdigit() and int(last) > 0):
                    norm = _normalize_line(raw)
                    rows.append(norm.split(";"))
            except Exception:
                continue
    return rows


# --------------------------------------------------------------


def _rows_to_standard_df(rows: List[List[str]]) -> pd.DataFrame:
    """
    Приводимо сирі рядки до стандартної моделі колонок:
    code, unicode, brand, name, stock, price
    (далі вже робимо множення/округлення/ренейм)
    """
    # Якщо у сирих рядках саме такий порядок — це спрацює відразу.
    # Якщо у когось інший порядок — для нього будемо використ. suppliers.yaml (на наступному етапі).
    df = pd.DataFrame(rows)
    # підстрахуємося: візьмемо перші 6 колонок
    df = df.iloc[:, :6].copy()
    df.columns = ["code", "unicode", "brand", "name", "stock", "price"]
    # ціни: коми → крапки
    df["price"] = (
        df["price"]
        .astype(str)
        .str.replace(",", ".", regex=False)
    )
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    # stock → int (STAN/>5 вже нормалізовані)
    df["stock"] = pd.to_numeric(df["stock"], errors="coerce").fillna(0).astype(int)
    return df


def _apply_pricing(df: pd.DataFrame, factor: float, currency_out: str, rate: float,
                   rounding: Dict[str, int]) -> pd.Series:
    """
    Повертає серію з фінальною колонкою ціни (імена дамо далі).
    EUR: price * factor
    UAH: price * factor * rate
    """
    base = df["price"].fillna(0)
    if currency_out == "UAH":
        val = base * factor * float(rate)
        digits = int(rounding.get("UAH", 0))
    else:
        val = base * factor
        digits = int(rounding.get("EUR", 2))
    return val.round(digits)


def _build_output_df(
        df_std: pd.DataFrame,
        price_final: pd.Series,
        columns_cfg: List[Dict[str, str]],
        supplier_id: Optional[int],
) -> pd.DataFrame:
    """
    Збираємо вихідний DataFrame у потрібному порядку і з правильними шапками.
    columns_cfg елементи виду: { from: code|unicode|brand|name|stock|price|supplier_id, header: "..." }
    """
    out_cols = {}
    temp = df_std.copy()
    temp["supplier_id"] = supplier_id if supplier_id is not None else None
    temp["price"] = price_final

    for col in columns_cfg:
        src = col["from"]
        hdr = col["header"]
        if src not in temp.columns:
            # якщо просили price_EUR/price_UAH через 'from: price' — ми вже їх у temp["price"] поклали
            # supplier_id може бути None — все одно створимо колонку
            temp[src] = temp.get(src, None)
        out_cols[hdr] = temp[src]

    out_df = pd.DataFrame(out_cols)
    return out_df


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
        csv_cfg: Dict[str, Any] | None = None,
        rate: float = 1.0,
) -> Tuple[str, str]:
    """
    Повний цикл: FTP → unzip → normalize → calc → export (xlsx/csv) → upload R2 → cleanup tmp
    Повертає (key, url) в R2.
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
    # 3) normalize → standard df
    rows = raw_csv_to_rows(csv_path)
    df_std = _rows_to_standard_df(rows)

    # 4) обчислення ціни й округлення
    price_final = _apply_pricing(df_std, factor=factor, currency_out=currency_out, rate=rate, rounding=rounding)

    # 5) збірка вихідного фрейму під columns
    out_df = _build_output_df(df_std, price_final, columns_cfg=columns, supplier_id=supplier_id)

    # 6) експорт (xlsx / csv)
    out_dir = tmp_dir
    ext = "xlsx" if format_.lower() == "xlsx" else "csv"
    out_path = out_dir / f"{supplier_code}_{stamp}.{ext}"

    if ext == "xlsx":
        out_df.to_excel(out_path, index=False, engine="xlsxwriter")
        content_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    else:
        delim = (csv_cfg or {}).get("delimiter", ";")
        header = bool((csv_cfg or {}).get("header", True))
        out_df.to_csv(out_path, index=False, sep=delim, header=header, encoding="utf-8")
        content_type = "text/csv"

    # 7) upload → R2 (та локальне очищення за префіксом)
    storage = StorageClient()
    prefix = r2_prefix  # вже з підставленим {supplier}
    key = f"{prefix}{supplier_code}_{stamp}.{ext}"

    # виберемо ліміт keep_last з ENV залежно від типу префікса
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
