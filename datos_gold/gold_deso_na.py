from utils.conexion_postgre import get_engine
from utils.funciones import sync_gold

def run_gold_deso_na():

    # CONFIG
    SILVER_SCHEMA = "silver"
    SILVER_TABLE = "silver_desocupacion_nacional"

    GOLD_SCHEMA = "gold"
    GOLD_TABLE = "gold_desocupacion_nacional"

    # CONEXIÓN
    engine = get_engine()
    conn = engine.raw_connection()
    cursor = conn.cursor()

    # Funcion incremnetal esta en utils.funciones
    sync_gold(
        silver_schema="silver",
        silver_table="silver_desocupacion_nacional",
        gold_schema="gold",
        gold_table="gold_desocupacion_nacional",
        columns=["desocupacion_nacional"],
    )