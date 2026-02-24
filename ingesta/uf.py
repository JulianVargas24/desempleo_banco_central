import bcchapi
from utils.conexion_postgre import get_engine
from utils.funciones import truncate_table


def run_bronze_uf():
    # Incluyendo credenciales explícitamente
    siete = bcchapi.Siete(file="credenciales.txt")

    series_code = "F073.UFF.PRE.Z.D"

    df = siete.cuadro(
        series=[series_code],
        nombres=["uf"],
        desde="2015-01-01",
        frecuencia="D",
        observado={"uf": "last"},
    )

    df = df.reset_index()
    df.columns = ["fecha", "uf"]

    # Conexión postgresql
    engine = get_engine()

    # Borrar datos existentes de la tabla
    truncate_table(engine, "bronze", "bronze_uf")

    # Ingestar a postgresql
    df.to_sql(
        name="bronze_uf", schema="bronze", con=engine, if_exists="append", index=False
    )

    print("✅ Bronze uf cargado correctamente")
