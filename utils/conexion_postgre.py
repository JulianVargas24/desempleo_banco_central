from sqlalchemy import create_engine

def get_engine():
    DB_USER = "postgres"
    DB_PASSWORD = "2508"
    DB_HOST = "localhost"
    DB_PORT = "5432"
    DB_NAME = "chile_labor_data"

    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    return engine