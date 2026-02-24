from utils.funciones import sync_gold


def run_gold_fuerza_trabajo():

    # Funcion incremnetal esta en utils.funciones
    sync_gold(
        silver_schema="silver",
        silver_table="silver_fuerza_trabajo",
        gold_schema="gold",
        gold_table="gold_fuerza_trabajo",
        columns=["total", "mujeres", "hombres"],
    )
