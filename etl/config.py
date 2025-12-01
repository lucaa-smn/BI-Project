import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()  # liest .env ein


def _get_env(name: str) -> str:
    value = os.getenv(name)
    if value is None:
        raise RuntimeError(f"Required env var {name} is not set")
    return value


DB_HOST = _get_env("DB_HOST")
DB_PORT = _get_env("DB_PORT")
DB_NAME = _get_env("DB_NAME")
DB_USER = _get_env("DB_USER")
DB_PASSWORD = _get_env("DB_PASSWORD")

DB_URI = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


def get_engine():
    return create_engine(DB_URI)
