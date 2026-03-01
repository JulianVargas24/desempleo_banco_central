from ingesta.deso_na import run_bronze_deso_na
from ingesta.deso_re import run_bronze_deso_re
from ingesta.fuerza_trabajo import run_bronze_fuerza_trabajo
from ingesta.imacec import run_bronze_imacec
from ingesta.ipc import run_bronze_ipc
from ingesta.pbi import run_bronze_pbi
from ingesta.uf import run_bronze_uf
import logging

logger = logging.getLogger(__name__)


def run_bronze():
    run_bronze_deso_na()
    run_bronze_deso_re()
    run_bronze_fuerza_trabajo()
    run_bronze_imacec()
    run_bronze_ipc()
    run_bronze_pbi()
    run_bronze_uf()

    logger.info("***CARGA DE BRONZE COMPLETADA***")


if __name__ == "__main__":
    run_bronze()
