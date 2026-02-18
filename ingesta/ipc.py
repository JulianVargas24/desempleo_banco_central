import bcchapi
from utils.conexion_postgre import get_engine
from utils.funciones import truncate_table

# Incluyendo credenciales explícitamente
siete = bcchapi.Siete(file="credenciales.txt")

series_code = "F074.IPC.VAR.Z.Z.C.M"

df = siete.cuadro(
    series=[series_code],
    nombres=["ipc"],
    desde="2015-01-01",
    frecuencia="ME",
    observado={"ipc":"last"}
)

df = df.reset_index()
df.columns = ["fecha", "ipc"]

# Conexión postgresql
engine = get_engine()

# Borrar datos existentes de la tabla
truncate_table(engine, "bronze", "bronze_ipc")

# Ingestar a postgresql
df.to_sql(
    name="bronze_ipc",
    schema="bronze",
    con=engine,
    if_exists="append",
    index=False
)

print(df)