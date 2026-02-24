import os
from sqlalchemy import create_engine


def get_engine():
    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        raise ValueError("DATABASE_URL no está configurada")

    return create_engine(database_url)
