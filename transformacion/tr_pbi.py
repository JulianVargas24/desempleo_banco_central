import pandas as pd
from utils.conexion_postgre import get_engine
from utils.funciones import truncate_table

def run_silver_pbi():
    engine = get_engine()
    query = """SELECT * FROM bronze.bronze_pbi"""

    df = pd.read_sql(query, engine)

    # Contar repetidos en la columna id
    """df2 = df["id"].duplicated().sum()"""

    # Reemplazar valores nullos y vacios de fecha
    df["fecha"] = (
        df["fecha"]
        .replace("", pd.NA)
        .fillna("1900-01-01")
    )

    # Dejar solo 2 decimales a imacec
    cols_to_round = ["pbi"]
    df[cols_to_round] = df[cols_to_round].round(2)

    # Borrar datos existentes de la tabla
    truncate_table(engine, "silver", "silver_pbi")

    # Llevar los datos a silver
    df.to_sql(
        name="silver_pbi",
        schema="silver",
        con=engine,
        if_exists="append",
        index=False
    )

    print("✅ Silver pbi cargado correctamente")