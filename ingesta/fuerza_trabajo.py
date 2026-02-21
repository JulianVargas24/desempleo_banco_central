import bcchapi
from utils.conexion_postgre import get_engine
from utils.funciones import truncate_table

def run_bronze_fuerza_trabajo():
    # Incluyendo credenciales explícitamente
    siete = bcchapi.Siete(file="credenciales.txt")

    series_code = ["F049.FTR.PMT.INE9.01.M", "F049.FTR.PMT.INE.D03.M", "F049.FTR.PMT.INE.D02.M"]

    df = siete.cuadro(
        series=series_code,
        nombres=["total", "mujeres", "hombres"],
        desde="2015-01-01",
        frecuencia="ME",
        observado={"total":"last", "mujeres":"last", "hombres":"last"}
    )

    # Reset index para tener fecha como columna
    df = df.reset_index()
    df.columns = ["fecha", "total", "mujeres", "hombres"]

    # Conexión postgresql
    engine = get_engine()

    # Borrar datos existentes de la tabla
    truncate_table(engine, "bronze", "bronze_fuerza_trabajo")

    # Ingestar postgresql
    df.to_sql(
        name="bronze_fuerza_trabajo",
        con=engine,
        schema= "bronze",
        if_exists="append",
        index=False
    )

    print("✅ Bronze fuerza trabajo cargado correctamente")
