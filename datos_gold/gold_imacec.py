from utils.conexion_postgre import get_engine
from utils.funciones import sync_gold

def run_gold_imacec():
    # CONFIG
    SILVER_SCHEMA = "silver"
    SILVER_TABLE = "silver_imacec"

    GOLD_SCHEMA = "gold"
    GOLD_TABLE = "gold_imacec"

    # CONEXIÓN
    engine = get_engine()
    conn = engine.raw_connection()
    cursor = conn.cursor()

    # Funcion incremnetal esta en utils.funciones
    def run_gold_imacec():
        sync_gold(
        silver_schema="silver",
        silver_table="silver_imacec",
        gold_schema="gold",
        gold_table="gold_imacec",
        columns=["imacec"],
    )