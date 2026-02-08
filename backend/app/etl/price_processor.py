import os
import re
import gzip
import shutil
import yaml
import ftplib
from datetime import datetime
from typing import Tuple, List, Dict, Any, Optional
from pathlib import Path

import pandas as pd
# --- Імпорт text для безпечних SQL-запитів ---
from sqlalchemy import create_engine, text

from app.services.paths import TEMP_DIR
from app.services.storage import StorageClient


# ----------------------- FTP / unzip -----------------------
# def download_file_from_ftp(remote_path: str, local_path: Path) -> None:
#     host = os.getenv("FTP_HOST")
#     user = os.getenv("FTP_USER")
#     pwd = os.getenv("FTP_PASS")
#     if not all([host, user, pwd]):
#         raise RuntimeError("FTP credentials are missing in .env")
#
#     # допоміжний виконавець
#     def _retr(ftp):
#         ftp.set_pasv(True)  # як у FileZilla (PASV)
#         ftp.login(user, pwd)
#         local_path.parent.mkdir(parents=True, exist_ok=True)
#         with open(local_path, "wb") as f:
#             ftp.retrbinary(f"RETR " + remote_path, f.write)
#         ftp.quit()
#
#     # 1) спроба через Explicit TLS (FTPS)
#     try:
#         ftps = ftplib.FTP_TLS(host, timeout=20)
#         ftps.auth()  # AUTH TLS
#         ftps.prot_p()  # шифрувати data channel
#         _retr(ftps)
#         return
#     except ftplib.all_errors as e_tls:
#         # 2) якщо TLS не доступний — пробуємо звичайний FTP
#         try:
#             ftp = ftplib.FTP(host, timeout=20)
#             _retr(ftp)
#             return
#         except ftplib.all_errors as e_plain:
#             # показати, що пробували обидва варіанти
#             raise RuntimeError(f"FTP/FTPS failed. FTPS: {e_tls}; FTP: {e_plain}")

# ----------------------- FTP / unzip -----------------------
def download_file_from_ftp(remote_path: str, local_path: Path, supplier: str) -> None:
    """
    Завантажує файл з FTP, використовуючи динамічні секрети з .env
    на основі імені постачальника (напр. AUTOPARTNER_FTP_HOST).
    """
    # 1) Готуємо префікс для пошуку в .env (напр. "AUTOPARTNER")
    prefix = supplier.upper().replace(" ", "_")

    # 2) Витягуємо специфічні налаштування для цього постачальника
    host = os.getenv(f"{prefix}_FTP_HOST")
    user = os.getenv(f"{prefix}_FTP_USER")
    pwd = os.getenv(f"{prefix}_FTP_PASS")

    # Перевірка: якщо в .env забули прописати дані для цього постачальника
    if not all([host, user, pwd]):
        raise RuntimeError(f"Credentials for {prefix} are missing in .env. "
                           f"Please add {prefix}_FTP_HOST, {prefix}_FTP_USER, {prefix}_FTP_PASS.")

    print(f"[INFO] Connecting to FTP for {prefix} ({host})...")

    # Допоміжний виконавець (залишається майже без змін, але використовує локальні host/user/pwd)
    def _retr(ftp):
        ftp.set_pasv(True)  # Режим PASV (як у FileZilla)
        ftp.login(user, pwd)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        with open(local_path, "wb") as f:
            ftp.retrbinary(f"RETR {remote_path}", f.write)
        ftp.quit()

    # 1) Спроба через Explicit TLS (FTPS) - більш безпечно
    try:
        ftps = ftplib.FTP_TLS(host, timeout=20)
        ftps.auth()
        ftps.prot_p()
        _retr(ftps)
        print(f"[SUCCESS] Downloaded via FTPS: {remote_path}")
        return
    except ftplib.all_errors as e_tls:
        # 2) Якщо TLS не доступний — пробуємо звичайний FTP
        try:
            print(f"[WARN] FTPS failed for {prefix}, trying plain FTP...")
            ftp = ftplib.FTP(host, timeout=20)
            _retr(ftp)
            print(f"[SUCCESS] Downloaded via FTP: {remote_path}")
            return
        except ftplib.all_errors as e_plain:
            raise RuntimeError(f"FTP/FTPS failed for {prefix}. TLS Error: {e_tls}; Plain Error: {e_plain}")



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
    Нормалізація рядка для «пробільних» форматів.
    """
    repl = str(gt5_to if gt5_to is not None else 10)
    line = re.sub(r">\s*5", repl, line)
    m = re.search(r"\w\s\w*\s\w", line)
    if m:
        line = re.sub(r"\s", "", line, count=1)
    line = re.sub(r"\s", ";", line)
    return line


# def raw_csv_to_rows(
#         input_csv: Path,
#         *,
#         stock_index: Optional[int],
#         stock_header_token: str = "STAN",
#         gt5_to: Optional[int] = None,
#         skip_rows: int = 0,
#         normalize_mode: str = "spaces",  # "spaces" | "csv"
# ) -> List[List[str]]:
#     """
#     Читає сирий CSV і повертає рядки (list[str]).
#     """
#     rows: List[List[str]] = []
#     with open(input_csv, "r", encoding="utf-8", errors="ignore") as f:
#         for i, raw in enumerate(f):
#             if i < skip_rows:
#                 continue
#             raw = raw.strip()
#             if not raw:
#                 continue
#
#             if normalize_mode == "csv":
#                 parts = raw.split(";")
#             else:
#                 norm = _normalize_line_with_cfg(raw, gt5_to=gt5_to)
#                 parts = norm.split(";")
#
#             if not parts:
#                 continue
#
#             idx = stock_index if stock_index is not None else (len(parts) - 1)
#             if idx < 0 or idx >= len(parts):
#                 continue
#
#             val = (parts[idx] or "").strip()
#
#             # пропускаємо службовий заголовок стоку
#             if val.lower() == (stock_header_token or "").lower():
#                 continue
#
#             # нормалізуємо '>5' у числове значення
#             if gt5_to is not None and (val.startswith(">") or val.replace(" ", "").startswith(">")):
#                 val = str(gt5_to)
#                 parts[idx] = val
#
#             try:
#                 if float(val) <= 0:
#                     continue
#             except ValueError:
#                 continue
#
#             rows.append(parts)
#     return rows

def raw_csv_to_rows(
        input_csv: Path,
        *,
        stock_index: Optional[int],
        stock_header_token: str = "STAN",
        gt5_to: Optional[int] = None,
        skip_rows: int = 0,
        normalize_mode: str = "spaces",
) -> List[List[str]]:
    """
    Читає сирий CSV. Якщо stock_index=None, повертає всі рядки без фільтрації залишків.
    """
    rows: List[List[str]] = []

    # Використовуємо cp1250 для польських прайсів, щоб не було помилок декодування
    with open(input_csv, "r", encoding="cp1250", errors="replace") as f:
        for i, raw in enumerate(f):
            if i < skip_rows:
                continue
            raw = raw.strip()
            if not raw:
                continue

            # Розбиваємо рядок на частини
            if normalize_mode == "csv":
                parts = raw.split(";")
            else:
                norm = _normalize_line_with_cfg(raw, gt5_to=gt5_to)
                parts = norm.split(";")

            if not parts:
                continue

            # --- ГОЛОВНА ЗМІНА ТУТ ---
            # Якщо ми не вказали індекс стоку (як для файлу цін),
            # ми просто додаємо рядок і йдемо далі, не перевіряючи числа.
            if stock_index is None:
                rows.append(parts)
                continue

            # --- ЛОГІКА ДЛЯ ФАЙЛУ ЗАЛИШКІВ (де індекс вказано) ---
            idx = stock_index
            if idx < 0 or idx >= len(parts):
                continue

            val = (parts[idx] or "").strip()

            # Пропускаємо заголовки типу "STAN"
            if val.lower() == (stock_header_token or "").lower():
                continue

            # Нормалізуємо '>5'
            if gt5_to is not None and ">" in val:
                val = str(gt5_to)
                parts[idx] = val

            # Перевірка на число (тільки для файлу залишків!)
            try:
                if float(val) <= 0:
                    continue
            except ValueError:
                # Якщо в колонці залишку не число — ігноруємо цей рядок
                continue

            rows.append(parts)

    return rows


def _rows_to_standard_df(rows: List[List[str]], colmap: Dict[str, int]) -> pd.DataFrame:
    """
    Приводимо сирі рядки до стандартної моделі колонок.
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
            stock = int(float(stock_s))
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
    Обчислює фінальну ціну.
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
    Збирає вихідний DataFrame.
    """
    temp = df_std.copy()
    temp["supplier_id"] = supplier_id if supplier_id is not None else None
    temp["price"] = price_final

    out_cols: Dict[str, pd.Series] = {}
    for col in columns_cfg:
        src = col["from"]
        hdr = col["header"]
        if src not in temp.columns:
            temp[src] = temp.get(src, None)
        out_cols[hdr] = temp[src]

    return pd.DataFrame(out_cols)


# ----------------------- Materialize to CSV -----------------------

# def _materialize_to_csv(remote_path: str, tmp_dir: Path) -> tuple[Path, list[Path]]:
#     """
#     Приводить будь-яке джерело до локального CSV.
#     """
#     cleanup: list[Path] = []
#
#     if os.path.exists(remote_path):
#         p = Path(remote_path)
#         if p.suffix.lower() == ".csv":
#             return p, cleanup
#         if p.suffix.lower() == ".gz":
#             csv_out = tmp_dir / f"{p.stem}"
#             if csv_out.suffix.lower() != ".csv":
#                 csv_out = csv_out.with_suffix(".csv")
#             unzip_gz_file(p, csv_out)
#             cleanup.append(csv_out)
#             return csv_out, cleanup
#         raise ValueError(f"Unsupported local file type: {p.suffix}")
#     else:
#         stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
#         gz_tmp = tmp_dir / f"ftp_{stamp}.csv.gz"
#         csv_tmp = tmp_dir / f"ftp_{stamp}.csv"
#         download_file_from_ftp(remote_path, gz_tmp)
#         unzip_gz_file(gz_tmp, csv_tmp)
#         cleanup.extend([gz_tmp, csv_tmp])
#         return csv_tmp, cleanup

def _materialize_to_csv(remote_path: str, tmp_dir: Path, supplier: str) -> tuple[Path, list[Path]]:
    """
    Завантажує файл (з локального диска або FTP) та готує його до читання.
    Тепер враховує назву постачальника та тип файлу (.csv або .gz).
    """
    cleanup: list[Path] = []

    # 1) Робота з локальним файлом (для тестів)
    if os.path.exists(remote_path):
        p = Path(remote_path)
        if p.suffix.lower() == ".csv":
            return p, cleanup
        if p.suffix.lower() == ".gz":
            csv_out = tmp_dir / f"{p.stem}.csv"
            unzip_gz_file(p, csv_out)
            cleanup.append(csv_out)
            return csv_out, cleanup
        raise ValueError(f"Unsupported local file type: {p.suffix}")

    # 2) Робота з FTP
    else:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = Path(remote_path).name

        # Перевіряємо, чи файл заархівований
        is_gz = remote_path.lower().endswith(".gz")

        # Створюємо шлях для завантаження
        download_path = tmp_dir / f"ftp_{stamp}_{filename}"

        # --- ВИКЛИК ОНОВЛЕНОГО ЗАВАНТАЖУВАЧА ---
        # Передаємо supplier, щоб функція знала, які паролі брати з .env
        download_file_from_ftp(remote_path, download_path, supplier)

        if is_gz:
            # Якщо це архів — розпаковуємо
            csv_tmp = download_path.with_suffix(".csv")
            if csv_tmp == download_path:  # про всяк випадок, щоб не затерти
                csv_tmp = download_path.parent / (download_path.name + "_unzipped.csv")

            unzip_gz_file(download_path, csv_tmp)

            # Додаємо обидва файли в чергу на видалення
            cleanup.extend([download_path, csv_tmp])
            return csv_tmp, cleanup
        else:
            # Якщо це звичайний CSV — просто повертаємо його
            cleanup.append(download_path)
            return download_path, cleanup


# ----------------------- Main pipeline -----------------------

def process_one_price(
        remote_gz_path: Optional[str],
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
        delete_input_after: bool = False,
        additional_files: Optional[Dict[str, str]] = None,
) -> Tuple[str, str]:
    """
    Повний цикл обробки одного прайсу.
    """
    tmp_dir = TEMP_DIR
    tmp_dir.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now().strftime("%Y%m%d_%H%M")
    supplier_code_str = supplier.lower()

    # --- 0) MATERIALIZE (Завантаження файлів) ---
    cleanup_paths = []
    local_files = {}

    # # Перевіряємо: якщо прийшов словник з файлами, обробляємо його
    # if additional_files:
    #     print(f"[INFO] Materializing multiple files: {list(additional_files.keys())}")
    #     local_files = {}
    #     for key, r_path in additional_files.items():
    #         # Завантажуємо та розпаковуємо кожен файл окремо
    #         l_path, c_paths = _materialize_to_csv(r_path, tmp_dir)
    #         local_files[key] = l_path
    #         cleanup_paths.extend(c_paths)
    #
    #     # Для подальшої обробки (normalize) вибираємо головний файл.
    #     # Зазвичай це файл з ключем "prices". Якщо його немає — беремо перший ліпший.
    #     csv_path = local_files.get("prices") or list(local_files.values())[0]
    #
    # elif remote_gz_path:
    #     # Стара логіка для одного файлу
    #     csv_path, c_paths = _materialize_to_csv(remote_gz_path, tmp_dir)
    #     cleanup_paths.extend(c_paths)
    # else:
    #     raise ValueError("No input files provided (remote_gz_path and additional_files are both empty)")

    if additional_files:
        print(f"[INFO] 📥 Завантаження кількох файлів для {supplier}...")
        for key, r_path in additional_files.items():
            l_path, c_paths = _materialize_to_csv(r_path, tmp_dir, supplier)
            local_files[key] = l_path
            cleanup_paths.extend(c_paths)
    elif remote_gz_path:
        csv_path, c_paths = _materialize_to_csv(remote_gz_path, tmp_dir, supplier)
        local_files["prices"] = csv_path
        cleanup_paths.extend(c_paths)
    else:
        raise ValueError("No input files provided")

    # --- 1) ПІДГОТОВКА ---
    sup_cfg = _load_supplier_cfg(supplier)
    layout = sup_cfg.get("raw_layout", {}) or {}
    colmap: Dict[str, int] = (layout.get("columns") or {})


    # Збираємо параметри читання, щоб не дублювати їх для кожного файлу
    read_params = {
        "stock_index": layout.get("stock_index"),
        "stock_header_token": layout.get("stock_header_token", "STAN"),
        "gt5_to": layout.get("gt5_to"),
        "skip_rows": (sup_cfg.get("preprocess") or {}).get("skip_rows", 0),
        "normalize_mode": (sup_cfg.get("normalize") or {}).get("mode", "spaces"),
    }

    # 2) ВИКОНАННЯ МЕРДЖУ ДЛЯ ТЕСТУВАННЯ
    if "prices" in local_files and "stock" in local_files:
        print(f"[INFO] 🧩 Режим МЕРДЖУ: Об'єднуємо ціни та залишки...")

        # # 👇 ВСТАВЛЯЙ СЮДИ ЦЕЙ БЛОК:
        # try:
        #     with open(local_files["prices"], 'r', encoding='utf-8', errors='ignore') as f:
        #         head = [f.readline().strip() for _ in range(5)]
        #     print(f"DEBUG: ПЕРШІ 5 РЯДКІВ ПРАЙСУ: {head}")
        # except Exception as e:
        #     print(f"DEBUG ERROR: {e}")
        # # 👆 КІНЕЦЬ БЛОКУ

        # 1. Читаємо файл цін
        rows_p = raw_csv_to_rows(local_files["prices"], **{**read_params, "stock_index": None})
        df_p = _rows_to_standard_df(rows_p, colmap)

        # 2. Читаємо файл залишків
        rows_s = raw_csv_to_rows(local_files["stock"], **read_params)
        df_s = _rows_to_standard_df(rows_s, colmap)

        # --- 👇 НОВИЙ БЛОК: АГРЕГАЦІЯ СТОКУ 👇 ---
        # Групуємо по артикулу і сумуємо залишки
        print(f"[INFO] 🔄 Підсумовуємо залишки для {len(df_s)} рядків...")
        df_s = df_s.groupby("code", as_index=False).agg({"stock": "sum"})
        print(f"[INFO] ✅ Після об'єднання складів залишилося {len(df_s)} унікальних артикулів.")
        # ------------------------------------------


        print(f"DEBUG: К-сть рядків у цінах: {len(df_p)}")
        print(f"DEBUG: К-сть рядків у залишках: {len(df_s)}")


        # 3. ВЛАСНЕ МЕРДЖ (Inner Join)
        # Ми беремо df_p (ціни), видаляємо там технічну колонку stock (вона пуста)
        # І приєднуємо реальний stock з df_s по колонці 'code'
        df_std = pd.merge(
            df_p.drop(columns=["stock"]),  # Викидаємо пустий сток з файлу цін
            df_s[["code", "stock"]],   # Беремо тільки код і реальний сток з файлу залишків
            on="code",
            how="inner"
        )
        print(f"[INFO] ✅ Об'єднання завершено: {len(df_std)} позицій")

        # Додатковий мердж для БРЕНДІВ (якщо є файл)
        if "brands" in local_files:
            print(f"[INFO] 🏷️  Додаємо повні назви брендів...")
            df_brands = pd.read_csv(local_files["brands"], sep=";", names=["short_name", "full_name"], encoding="cp1250", quotechar='"', encoding_errors="replace")
            df_brands["short_name"] = df_brands["short_name"].astype(str).str.strip().str.upper()
            df_std["brand"] = df_std["brand"].astype(str).str.strip().str.upper()

            df_std = pd.merge(df_std, df_brands, left_on="brand", right_on="short_name", how="left")
            df_std["brand"] = df_std["full_name"].fillna(df_std["brand"])
            df_std = df_std.drop(columns=["short_name", "full_name"])

        print(f"[INFO] ✅ Злиття завершено. Разом позицій: {len(df_std)}")

    else:
        # Стара логіка для одного файлу (наприклад, Maxgear)
        csv_path = local_files.get("prices") or list(local_files.values())[0]
        rows = raw_csv_to_rows(csv_path, **read_params)
        df_std = _rows_to_standard_df(rows, colmap)



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

    # =================================================================
    # ЗМІНА (Вирішує Проблему 1): Розумне збереження в базу даних
    # =================================================================
    if "/site/" in r2_prefix and supplier_id is not None:
        try:
            print(f"[INFO] DB Trigger: Updating site prices for supplier ID {supplier_id}. Connecting to PostgreSQL...")
            # ВАЖЛИВО: Впишіть ваш пароль!
            db_password = "123456789"

            db_user = "postgres"
            db_host = "localhost"
            db_port = "5432"
            db_name = "postgres"

            db_url = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
            engine = create_engine(db_url)

            # КРОК А: Очищення старих даних ТІЛЬКИ цього постачальника
            print(f"[INFO] DB: Removing old records for supplier ID {supplier_id}...")
            with engine.connect() as conn:
                # НОВЕ: Перевіряємо, чи існує таблиця, перед видаленням
                from sqlalchemy import inspect
                inspector = inspect(engine)

                if inspector.has_table("product_catalog"):
                    # Таблиця є, можна видаляти старі записи
                    conn.execute(
                        text("DELETE FROM product_catalog WHERE supplier_id = :sup_id"),
                        {"sup_id": supplier_id}
                    )
                    conn.commit()
                    print(f"[INFO] DB: Old records deleted.")
                else:
                    # Таблиці немає, нічого видаляти. Вона створиться на наступному кроці.
                    print(f"[INFO] DB: Table 'product_catalog' does not exist yet. Skipping DELETE.")

            # КРОК Б: Додавання нових даних (append)
            print(f"[INFO] DB: Appending {len(out_df)} new rows for supplier ID {supplier_id}...")

            # --- 👇 ВСТАВЛЯЙ ЦЕЙ РЯДОК ТУТ 👇 ---
            # Видаляємо символ NUL (0x00), який PostgreSQL не приймає
            out_df = out_df.map(lambda x: x.replace('\x00', '') if isinstance(x, str) else x)
            # ------------------------------------

            # if_exists='append' додає дані до існуючої таблиці
            out_df.to_sql('product_catalog', con=engine, if_exists='append', index=False)

            print(f"[INFO] PostgreSQL: SUCCESS! Site prices for supplier ID {supplier_id} updated.")

        except Exception as e:
            print(f"\n[ERROR] PostgreSQL save failed!!!! Details: {e}\n")
    elif "/site/" in r2_prefix and supplier_id is None:
         print(f"\n[WARNING] DB Trigger skipped: Found '/site/' prefix but supplier_id is None.\n")
    # =================================================================


    # 4) export
    ext = "xlsx" if format_.lower() == "xlsx" else "csv"
    out_path = tmp_dir / f"{supplier_code_str}_{stamp}.{ext}"

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
    key = f"{prefix}{supplier_code_str}_{stamp}.{ext}"

    keep_last = 7
    if prefix.startswith("1_23/"):
        keep_last = int(os.getenv("R2_KEEP_123", "7"))
    elif prefix.startswith("1_27/"):
        keep_last = int(os.getenv("R2_KEEP_127", "7"))
    elif prefix.startswith("1_33/site/"):
        keep_last = int(os.getenv("R2_KEEP_133_SITE", "7"))
    elif prefix.startswith("1_33/exist/"):
        keep_last = int(os.getenv("R2_KEEP_133_EXIST", "7"))
    elif prefix.startswith("netto/"):
        keep_last = int(os.getenv("R2_KEEP_NETTO", "7"))

    url = storage.upload_file(
        local_path=str(out_path),
        key=key,
        content_type=content_type,
        cleanup_prefix=prefix,
        keep_last=keep_last,
    )

    # 6) local cleanup
    try:
        out_path.unlink(missing_ok=True)
        for p in cleanup_paths:
            p.unlink(missing_ok=True)
        if delete_input_after and os.path.exists(remote_gz_path):
            rp = Path(remote_gz_path)
            if rp.exists() and rp.resolve() not in [out_path.resolve(), *[c.resolve() for c in cleanup_paths]]:
                rp.unlink(missing_ok=True)
    except Exception:
        pass

    return key, url