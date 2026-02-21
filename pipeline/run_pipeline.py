from ingesta.run_bronze import run_bronze
from transformacion.run_silver import run_silver
from datos_gold.run_gold import run_gold

def main():
    run_bronze()
    run_silver()
    run_gold()

if __name__ == "__main__":
    main()