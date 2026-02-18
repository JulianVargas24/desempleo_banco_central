import bcchapi
from utils.conexion_postgre import get_engine
from utils.funciones import truncate_table

# Consumo api banco central
siete = bcchapi.Siete(file="credenciales.txt")
series_code = "F049.DES.TAS.INE9.10.M"

df = siete.cuadro(
    series=[series_code],
    nombres=["desocupacion_nacional"],
    desde="2015-01-01",
    frecuencia="ME",
    observado={"desocupacion_nacional": "last"}
)

# Reset index para tener fecha como columna
df = df.reset_index()
df.columns = ["fecha", "desocupacion_nacional"]

# Conexión postgresql
engine = get_engine()

# Borrar datos existentes de la tabla
truncate_table(engine, "bronze", "bronze_desocupacion_nacional")

# Ingestar a postgresql
df.to_sql(
    name="bronze_desocupacion_nacional",
    schema="bronze",
    con=engine,
    if_exists="append",
    index=False
)

print(df)