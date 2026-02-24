from sqlalchemy import text
from utils.conexion_postgre import get_engine


def test_db_connection():
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        assert result.scalar() == 1
