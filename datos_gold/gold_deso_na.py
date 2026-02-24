from utils.funciones import sync_gold


def run_gold_deso_na():

    # Funcion incremnetal esta en utils.funciones
    sync_gold(
        silver_schema="silver",
        silver_table="silver_desocupacion_nacional",
        gold_schema="gold",
        gold_table="gold_desocupacion_nacional",
        columns=["desocupacion_nacional"],
    )
