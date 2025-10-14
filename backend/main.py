# main.py

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Створюємо екземпляр нашого додатку
app = FastAPI()

# Налаштовуємо CORS, щоб твій React-додаток міг робити запити
# Обов'язково вкажи правильний порт твого Vite-сервера
origins = ["http://localhost:5173"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Створюємо перший "ендпоінт" (адресу)
# Коли хтось зайде на головну сторінку нашого API,
# він отримає це повідомлення
@app.get("/")
def read_root():
    return {"message": "API сервер для AutoParts працює!"}