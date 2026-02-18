import bcchapi

# Incluyendo credenciales explícitamente
siete = bcchapi.Siete("vargasjulian.2207@gmail.com", "Julioan99")

# Buscar series que contengan la palabra
df_series = siete.buscar("pbi")

print(df_series.head())