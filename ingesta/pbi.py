import bcchapi
from utils.conexion_postgre import get_engine
from utils.funciones import truncate_table


def run_bronze_pbi():
    # Incluyendo credenciales explícitamente
    siete = bcchapi.Siete(file="credenciales.txt")

    series_code = "F032.PIB.FLU.R.CLP.EP18.Z.Z.0.T"

    df = siete.cuadro(
        series=[series_code],
        nombres=["pbi"],
        desde="2015-01-01",
        frecuencia="QE",
        observado={"pbi": "last"},
    )

    df = df.reset_index()
    df.columns = ["fecha", "pbi"]

    # Conexión postgresql
    engine = get_engine()

    # Borrar datos existentes de la tabla
    truncate_table(engine, "bronze", "bronze_pbi")

    # Ingestar a postgresql
    df.to_sql(
        name="bronze_pbi", schema="bronze", con=engine, if_exists="append", index=False
    )

    print("✅ Bronze pbi cargado correctamente")
