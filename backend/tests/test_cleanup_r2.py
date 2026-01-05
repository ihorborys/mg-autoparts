from dotenv import load_dotenv

load_dotenv()

from datetime import datetime
from app.services.storage import StorageClient


def test_cleanup():
    storage = StorageClient()
    supplier = "ap_gdansk"
    prefix = f"1_23/{supplier}/"  # папка для тесту
    test_count = 10  # створимо 10 тестових об'єктів
    keep_last = 7  # хочемо залишити тільки 7

    # 1️⃣ створимо "порожні" файли у R2
    for i in range(test_count):
        key = f"{prefix}test_{i}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        print(f"📤 Uploading {key}")
        storage.s3.put_object(
            Bucket=storage.bucket,
            Key=key,
            Body=f"test file {i}".encode("utf-8"),
            ContentType="text/plain",
        )

    # 2️⃣ викликаємо очищення
    storage.cleanup_old_files(prefix, keep=keep_last)

    # 3️⃣ перевіряємо, скільки залишилось
    items = storage._list_all_objects(prefix)
    print(f"\n✅ {len(items)} files remaining (expected {keep_last}):")
    for obj in items:
        print(" -", obj["Key"])


if __name__ == "__main__":
    test_cleanup()
