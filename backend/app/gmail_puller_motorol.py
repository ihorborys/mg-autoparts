# -*- coding: utf-8 -*-
"""
Gmail puller для MOTOROL:
- шукає листи з вкладеннями *.zip
- завантажує 09033.cennik.zip
- розпаковує, форматує CSV
- викликає pipeline process_all_prices("MOTOROL", <formatted_csv>)
Запуск: python -m backend.gmail_puller_motorol
"""

from __future__ import annotations

import base64
import json
import zipfile
from pathlib import Path
from typing import Optional, List, Dict, Any

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from paths import TEMP_DIR
from price_manager import process_all_prices

# --------- Налаштування ---------
SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
# підлаштуй під точну адресу/тему відправника
GMAIL_QUERY = 'from:(motorol) has:attachment filename:zip newer_than:14d'

# Єдина тимчасова директорія
TMP_DIR = TEMP_DIR
STATE_FILE = TMP_DIR / "gmail_puller_state.json"


# --------- Допоміжне ---------
def ensure_tmp():
    TMP_DIR.mkdir(parents=True, exist_ok=True)


def load_state() -> Dict:
    if STATE_FILE.exists():
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"processed": []}


def save_state(state: Dict):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def get_creds() -> Credentials:
    """
    Потрібні файли:
      - backend/credentials.json (OAuth client)
      - backend/token.json (згенерується після першого логіну у браузері)
    """
    cred_path = Path("backend/credentials.json")
    token_path = Path("backend/token.json")
    creds: Optional[Credentials] = None

    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            # перший запуск: відкриє браузер для OAuth
            from google_auth_oauthlib.flow import InstalledAppFlow
            flow = InstalledAppFlow.from_client_secrets_file(str(cred_path), SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token_path, "w", encoding="utf-8") as f:
            f.write(creds.to_json())

    return creds


def gmail_service() -> Any:
    creds = get_creds()
    return build("gmail", "v1", credentials=creds, cache_discovery=False)


def search_messages(service, q: str) -> List[Dict]:
    res = service.users().messages().list(userId="me", q=q, maxResults=10).execute()
    return res.get("messages", [])


def download_first_zip_attachment(service, msg_id: str, dest_dir: Path) -> Optional[Path]:
    msg = service.users().messages().get(userId="me", id=msg_id).execute()
    payload = msg.get("payload", {})
    parts = payload.get("parts", []) or []

    for part in parts:
        filename = part.get("filename") or ""
        if not filename.lower().endswith(".zip"):
            continue

        body = part.get("body", {})
        att_id = body.get("attachmentId")

        if not att_id:
            # інколи контент у data (inline)
            data = body.get("data")
            if data:
                raw = base64.urlsafe_b64decode(data.encode("utf-8"))
                out = dest_dir / filename
                with open(out, "wb") as f:
                    f.write(raw)
                return out
            continue

        att = service.users().messages().attachments().get(
            userId="me", messageId=msg_id, id=att_id
        ).execute()
        raw = base64.urlsafe_b64decode(att["data"].encode("utf-8"))
        out = dest_dir / filename
        with open(out, "wb") as f:
            f.write(raw)
        return out

    return None


def unzip_to_csv(zip_path: Path, extract_dir: Path) -> Path:
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)
    # шукаємо перший CSV (Motorol: 09033.cennik.csv)
    for p in extract_dir.iterdir():
        if p.suffix.lower() == ".csv":
            return p
    raise FileNotFoundError("CSV file not found inside zip.")


def format_motorol_csv(input_csv: Path, output_csv: Path) -> None:
    """
    Табуляція → ';', прибираємо '; ' → ';',
    фільтр по колонці 'stan' (row[-3]), заміна '>5' → '10'.
    """
    import csv, re
    formatted: List[List[str]] = []
    with open(input_csv, newline="", encoding="utf-8", errors="ignore") as csvfile:
        reader = csv.reader(csvfile, delimiter="\t")
        for row in reader:
            joined = ";".join(row)
            joined = re.sub(r";\s+", ";", joined)
            parts = joined.split(";")

            if len(parts) < 3:
                continue

            stock_idx = len(parts) - 3
            stock_val = parts[stock_idx] if 0 <= stock_idx < len(parts) else ""

            if stock_val == "stan" or stock_val == ">5" or stock_val != "0":
                joined2 = ";".join(parts)
                joined2 = re.sub(r">5", "10", joined2)
                formatted.append(joined2.split(";"))

    with open(output_csv, "w", newline="", encoding="utf-8") as out:
        writer = csv.writer(out, delimiter=";")
        writer.writerows(formatted)


def already_processed(state: Dict, msg_id: str) -> bool:
    return msg_id in state.get("processed", [])


def mark_processed(state: Dict, msg_id: str):
    s = set(state.get("processed", []))
    s.add(msg_id)
    state["processed"] = list(s)


# --------- Основний сценарій ---------
def handle_one_message(service, msg_id: str) -> Dict:
    ensure_tmp()
    zip_path = download_first_zip_attachment(service, msg_id, TMP_DIR)
    if not zip_path:
        return {"msg_id": msg_id, "status": "no-zip"}

    csv_raw = unzip_to_csv(zip_path, TMP_DIR)
    csv_fmt = TMP_DIR / f"MOTOROL_formatted_{zip_path.stem}.csv"
    format_motorol_csv(csv_raw, csv_fmt)

    # process_all_prices сприймає локальний шлях як "remote_gz_path"
    results = process_all_prices(supplier="MOTOROL", remote_gz_path=str(csv_fmt))

    # прибираємо тимчасові файли (форматований CSV видалить бекенд)
    try:
        zip_path.unlink(missing_ok=True)
        csv_raw.unlink(missing_ok=True)
    except Exception:
        pass

    return {"msg_id": msg_id, "status": "ok", "results": results}


def main():
    ensure_tmp()
    state = load_state()
    service = gmail_service()

    messages = search_messages(service, GMAIL_QUERY)
    if not messages:
        print("No messages found.")
        return

    for m in messages:
        msg_id = m["id"]
        if already_processed(state, msg_id):
            continue
        try:
            out = handle_one_message(service, msg_id)
            print("Processed:", out)
            mark_processed(state, msg_id)
            save_state(state)
        except Exception as e:
            print(f"Error processing {msg_id}: {e}")


if __name__ == "__main__":
    main()
