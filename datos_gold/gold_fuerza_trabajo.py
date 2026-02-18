import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from utils.conexion_postgre import get_engine


# ======================================================
# CONFIG
# ======================================================

SILVER_SCHEMA = "silver"
SILVER_TABLE = "silver_fuerza_trabajo"

GOLD_SCHEMA = "gold"
GOLD_TABLE = "gold_fuerza_trabajo"


# ======================================================
# CONEXIÓN
# ======================================================

engine = get_engine()
conn_url = engine.url

conn = psycopg2.connect(
    dbname=conn_url.database,
    user=conn_url.username,
    password=conn_url.password,
    host=conn_url.host,
    port=conn_url.port
)

cursor = conn.cursor()


# ======================================================
# 1️⃣ LEER SILVER COMPLETO
# ======================================================

query = f"""
    SELECT *
    FROM {SILVER_SCHEMA}.{SILVER_TABLE}
    ORDER BY fecha;
"""

df = pd.read_sql(query, engine)

if df.empty:
    raise ValueError("❌ Silver fuerza_trabajo está vacío.")

print(f"🔎 Filas leídas desde Silver fuerza_trabajo: {len(df)}")

# Eliminamos columnas técnicas
df = df.drop(columns=["id", "fecha_carga"])

# ======================================================
# 2️⃣ UPSERT MASIVO (MISMA LÓGICA QUE REGIONAL)
# ======================================================

data = list(df.itertuples(index=False, name=None))
columns = list(df.columns)

columns_sql = ", ".join(columns)

update_columns = ", ".join(
    [f"{col} = EXCLUDED.{col}" for col in columns if col != "fecha"]
)

upsert_sql = f"""
    INSERT INTO {GOLD_SCHEMA}.{GOLD_TABLE}
        ({columns_sql})
    VALUES %s
    ON CONFLICT (fecha)
    DO UPDATE SET
        {update_columns},
        fecha_carga = CURRENT_TIMESTAMP;
"""

execute_values(cursor, upsert_sql, data)

print("✅ UPSERT fuerza_trabajo completado.")


# ======================================================
# 3️⃣ DELETE SINCRONIZADO (MISMO PATRÓN)
# ======================================================

delete_sql = f"""
    DELETE FROM {GOLD_SCHEMA}.{GOLD_TABLE} g
    WHERE NOT EXISTS (
        SELECT 1
        FROM {SILVER_SCHEMA}.{SILVER_TABLE} s
        WHERE s.fecha = g.fecha
    );
"""

cursor.execute(delete_sql)

print("🗑️ Eliminación sincronizada completada.")


# ======================================================
# 4️⃣ COMMIT
# ======================================================

conn.commit()


# ======================================================
# 5️⃣ VALIDACIÓN
# ======================================================

cursor.execute(f"""
    SELECT COUNT(*), MIN(fecha), MAX(fecha)
    FROM {GOLD_SCHEMA}.{GOLD_TABLE};
""")

result = cursor.fetchone()

print("\n📊 Estado final GOLD fuerza_trabajo:")
print(f"Total filas: {result[0]}")
print(f"Desde: {result[1]}")
print(f"Hasta: {result[2]}")

cursor.close()
conn.close()

print("\n🚀 Sincronización total fuerza_trabajo completada.")
