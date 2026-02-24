from utils.funciones import sync_gold


def run_gold_imacec():

    # Funcion incremnetal esta en utils.funciones
    sync_gold(
        silver_schema="silver",
        silver_table="silver_imacec",
        gold_schema="gold",
        gold_table="gold_imacec",
        columns=["imacec"],
    )
