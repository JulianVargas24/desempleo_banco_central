import os
from sqlalchemy import create_engine


def get_engine():
    """
    Devuelve engine SQLAlchemy.

    Prioridad:
    1️⃣ Si existe AIRFLOW_CONN_NEON_POSTGRES (cuando corre en Airflow)
    2️⃣ Si existe DATABASE_URL (cuando corre local)
    """

    # 🔹 Caso Airflow (usa variable automática)
    airflow_conn = os.getenv("AIRFLOW_CONN_NEON_POSTGRES")
    if airflow_conn:
        return create_engine(airflow_conn)

    # 🔹 Caso local
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return create_engine(database_url)

    raise ValueError("No se encontró conexión a base de datos.")
