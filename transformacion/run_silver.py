from transformacion.tr_deso_na import run_silver_deso_na
from transformacion.tr_deso_re import run_silver_deso_re
from transformacion.tr_fuerza_trabajo import run_silver_fuerza_trabajo
from transformacion.tr_imacec import run_silver_imacec
from transformacion.tr_ipc import run_silver_ipc
from transformacion.tr_pbi import run_silver_pbi
from transformacion.tr_uf import run_silver_uf

def run_silver():
    run_silver_deso_na()
    run_silver_deso_re()
    run_silver_fuerza_trabajo()
    run_silver_imacec()
    run_silver_ipc()
    run_silver_pbi()
    run_silver_uf()

    print("***CARGA DE SILVER COMPLETADA***")

if __name__ == "__main__":
    run_silver()