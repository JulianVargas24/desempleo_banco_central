from utils.funciones import sync_gold


def run_gold_uf():

    # Funcion incremnetal esta en utils.funciones
    sync_gold(
        silver_schema="silver",
        silver_table="silver_uf",
        gold_schema="gold",
        gold_table="gold_uf",
        columns=["uf"],
    )
