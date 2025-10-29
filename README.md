# MaxGear | Автозапчастини

BACKEND

Запустити сервер для AP_GDANSK:
uvicorn backend.app.main:app --reload
uvicorn app.main:app --reload

Gmail puller для MOTOROL:

- знаходить найновіший лист із вкладенням рівно "09033.cennik.zip"
- завантажує zip, розпаковує CSV, форматує
- запускає process_all_prices("MOTOROL", <formatted_csv>)
  Запуск (з кореня):   python -m backend.app.gmail_puller_motorol
  Запуск (з backend/): python -m app.gmail_puller_motorol

## License / Ліцензія

This project is proprietary. All rights reserved © 2025 Borys Ihor.  
Use of this code is prohibited without explicit permission.

Цей проєкт є приватним. Всі права захищені © 2025 Борис Ігор.  
Використання цього коду заборонене без письмового дозволу автора.

