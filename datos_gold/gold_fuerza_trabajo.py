from utils.conexion_postgre import get_engine
from utils.funciones import sync_gold

def run_gold_fuerza_trabajo():
    
    # CONFIG
    SILVER_SCHEMA = "silver"
    SILVER_TABLE = "silver_fuerza_trabajo"

    GOLD_SCHEMA = "gold"
    GOLD_TABLE = "gold_fuerza_trabajo"

    # CONEXIÓN
    engine = get_engine()
    conn = engine.raw_connection()
    cursor = conn.cursor()

    # Funcion incremnetal esta en utils.funciones
    def run_gold_fuerza_trabajo():
        sync_gold(
        silver_schema="silver",
        silver_table="silver_fuerza_trabajo",
        gold_schema="gold",
        gold_table="gold_fuerza_trabajo",
        columns=["total", "mujeres", "hombres"],
    )