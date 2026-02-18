import pandas as pd
from utils.conexion_postgre import get_engine
from utils.funciones import truncate_table

engine = get_engine()

query = """SELECT * FROM bronze.bronze_desocupacion_nacional"""

df = pd.read_sql(query, engine)

# Reemplazar valores nullos y vacios de fecha
df["fecha"] = (
    df["fecha"]
    .replace("", pd.NA)
    .fillna("1900-01-01")
)

# Dejar solo 2 decimales a columna desocupacion_nacional
df["desocupacion_nacional"] = df["desocupacion_nacional"].round(2)

# Borrar datos existentes de la tabla
truncate_table(engine, "silver", "silver_desocupacion_nacional")

# Llevar los datos a silver
df.to_sql(
    name="silver_desocupacion_nacional",
    schema="silver",
    con=engine,
    if_exists="append",
    index=False
)

print(df)

