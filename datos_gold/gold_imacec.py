import psycopg2
from utils.conexion_postgre import get_engine


# ======================================================
# CONFIG
# ======================================================

SILVER_SCHEMA = "silver"
SILVER_TABLE = "silver_imacec"

GOLD_SCHEMA = "gold"
GOLD_TABLE = "gold_imacec"


# ======================================================
# CONEXIÓN
# ======================================================

engine = get_engine()
conn = engine.raw_connection()
cursor = conn.cursor()

# ======================================================
# 1️⃣ VALIDAR QUE SILVER NO ESTÉ VACÍO
# ======================================================

cursor.execute(f"""
    SELECT COUNT(*)
    FROM {SILVER_SCHEMA}.{SILVER_TABLE};
""")

silver_count = cursor.fetchone()[0]

if silver_count == 0:
    raise ValueError("❌ Silver imacec está vacío. No se puede sincronizar.")

print(f"🔎 Filas en Silver imacec: {silver_count}")


# ======================================================
# 2️⃣ INSERT SOLO NUEVOS (NO QUEMA IDS)
# ======================================================

insert_sql = f"""
    INSERT INTO {GOLD_SCHEMA}.{GOLD_TABLE}
        (fecha, imacec)
    SELECT
        s.fecha,
        s.imacec
    FROM {SILVER_SCHEMA}.{SILVER_TABLE} s
    WHERE NOT EXISTS (
        SELECT 1
        FROM {GOLD_SCHEMA}.{GOLD_TABLE} g
        WHERE g.fecha = s.fecha
    );
"""

cursor.execute(insert_sql)
print("✅ INSERT de nuevos registros completado.")


# ======================================================
# 3️⃣ UPDATE SOLO SI CAMBIA EL VALOR
# ======================================================

update_sql = f"""
    UPDATE {GOLD_SCHEMA}.{GOLD_TABLE} g
    SET
        imacec = s.imacec,
        fecha_carga = CURRENT_TIMESTAMP
    FROM {SILVER_SCHEMA}.{SILVER_TABLE} s
    WHERE g.fecha = s.fecha
    AND g.imacec IS DISTINCT FROM s.imacec;
"""

cursor.execute(update_sql)
print("🔄 UPDATE de registros modificados completado.")


# ======================================================
# 4️⃣ DELETE REGISTROS QUE YA NO EXISTEN EN SILVER
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
print("🗑️ Eliminación de registros obsoletos completada.")


# ======================================================
# 5️⃣ COMMIT
# ======================================================

conn.commit()


# ======================================================
# 6️⃣ VALIDACIÓN FINAL
# ======================================================

cursor.execute(f"""
    SELECT COUNT(*), MIN(fecha), MAX(fecha)
    FROM {GOLD_SCHEMA}.{GOLD_TABLE};
""")

result = cursor.fetchone()

print("\n📊 Estado final GOLD IMACEC:")
print(f"Total filas: {result[0]}")
print(f"Desde: {result[1]}")
print(f"Hasta: {result[2]}")

cursor.close()
conn.close()

print("\n🚀 Sincronización incremental Silver → Gold imacec completada correctamente.")
