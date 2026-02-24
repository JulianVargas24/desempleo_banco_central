from utils.funciones import sync_gold


def run_gold_pbi():

    # Funcion incremnetal esta en utils.funciones
    sync_gold(
        silver_schema="silver",
        silver_table="silver_pbi",
        gold_schema="gold",
        gold_table="gold_pbi",
        columns=["pbi"],
    )
