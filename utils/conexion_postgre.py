import os
from sqlalchemy import create_engine


def get_engine():
    database_url = os.getenv("SECRET_DESEMPLEO")

    if not database_url:
        raise ValueError("SECRET_DESEMPLEO no está configurada")

    return create_engine(database_url)
