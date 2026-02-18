import bcchapi
from utils.conexion_postgre import get_engine
from utils.funciones import truncate_table

# Incluyendo credenciales explícitamente
siete = bcchapi.Siete(file="credenciales.txt")
series_code = "F032.IMC.IND.Z.Z.EP18.Z.Z.0.M"

df = siete.cuadro(
    series=[series_code],
    nombres=["imacec"],
    desde="2015-01-01",
    frecuencia="ME",
    observado={"imacec":"last"}
)

# Reset index para tener fecha como columna
df = df.reset_index()
df.columns = ["fecha", "imacec"]

# Conexión postgresql
engine = get_engine()

# Borrar datos existentes de la tabla
truncate_table(engine, "bronze", "bronze_imacec")

# Ingestar a postgresql
df.to_sql(
    name="bronze_imacec",
    schema="bronze",
    con=engine,
    if_exists="append",
    index=False
)

print(df)