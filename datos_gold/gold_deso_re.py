from utils.conexion_postgre import get_engine
from utils.funciones import sync_gold

def run_gold_deso_re():

    # CONFIG
    SILVER_SCHEMA = "silver"
    SILVER_TABLE = "silver_desocupacion_regional"

    GOLD_SCHEMA = "gold"
    GOLD_TABLE = "gold_desocupacion_regional"

    # CONEXIÓN
    engine = get_engine()
    conn = engine.raw_connection()
    cursor = conn.cursor()

    # Funcion incremnetal esta en utils.funciones
    sync_gold(
        silver_schema="silver",
        silver_table="silver_desocupacion_regional",
        gold_schema="gold",
        gold_table="gold_desocupacion_regional",
        columns=[
            "arica_parinacota",
            "tarapaca",
            "antofagasta",
            "atacama",
            "coquimbo",
            "valparaiso",
            "metropolitana",
            "ohiggins",
            "maule",
            "nuble",
            "bio_bio",
            "araucania",
            "los_rios",
            "los_lagos",
            "aysen",
            "magallanes",
        ],
    )