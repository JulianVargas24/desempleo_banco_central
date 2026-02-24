from utils.funciones import sync_gold


def run_gold_deso_re():

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
