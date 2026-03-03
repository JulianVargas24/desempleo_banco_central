import pandas as pd
from utils.conexion_postgre import get_engine
from utils.funciones import truncate_table


def run_silver_deso_na():

    engine = get_engine()

    query = """SELECT * FROM bronze.bronze_desocupacion_nacional"""
    df = pd.read_sql(query, engine)

    # Reemplazar valores nulos y vacíos
    df["fecha"] = df["fecha"].replace("", pd.NA).fillna("1900-01-01")

    # Redondear y poner nulos a 0
    cols = ["desocupacion_nacional", "desocupacion_mujeres", "desocupacion_hombres"]

    df[cols] = df[cols].round(2)
    df[cols] = df[cols].fillna(0)

    # Truncate Silver
    truncate_table(engine, "silver", "silver_desocupacion_nacional")

    # Insertar datos
    df.to_sql(
        name="silver_desocupacion_nacional",
        schema="silver",
        con=engine,
        if_exists="append",
        index=False,
    )

    print("✅ Silver desocupación nacional cargado correctamente")
