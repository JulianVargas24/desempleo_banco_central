from datos_gold.gold_deso_na import run_gold_deso_na
from datos_gold.gold_deso_re import run_gold_deso_re
from datos_gold.gold_fuerza_trabajo import run_gold_fuerza_trabajo
from datos_gold.gold_imacec import run_gold_imacec
from datos_gold.gold_ipc import run_gold_ipc
from datos_gold.gold_pbi import run_gold_pbi
from datos_gold.gold_uf import run_gold_uf


def run_gold():
    run_gold_deso_na()
    run_gold_deso_re()
    run_gold_fuerza_trabajo()
    run_gold_imacec()
    run_gold_ipc()
    run_gold_pbi()
    run_gold_uf()

    print("*** CARGA DE GOLD COMPLETADA ***")


if __name__ == "__main__":
    run_gold()
