import psycopg2
from utils.conexion_postgre import get_engine


# ======================================================
# CONFIG
# ======================================================

SILVER_SCHEMA = "silver"
SILVER_TABLE = "silver_desocupacion_regional"

GOLD_SCHEMA = "gold"
GOLD_TABLE = "gold_desocupacion_regional"


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
# 1️⃣ VALIDAR QUE SILVER NO ESTÉ VACÍO
# ======================================================

cursor.execute(f"""
    SELECT COUNT(*)
    FROM {SILVER_SCHEMA}.{SILVER_TABLE};
""")

silver_count = cursor.fetchone()[0]

if silver_count == 0:
    raise ValueError("❌ Silver regional está vacío. No se puede sincronizar.")

print(f"🔎 Filas en Silver regional: {silver_count}")


# ======================================================
# 2️⃣ INSERT SOLO NUEVOS (NO QUEMA IDS)
# ======================================================

insert_sql = f"""
    INSERT INTO {GOLD_SCHEMA}.{GOLD_TABLE} (
        fecha,
        arica_parinacota, tarapaca, antofagasta, atacama,
        coquimbo, valparaiso, metropolitana, ohiggins,
        maule, nuble, bio_bio, araucania,
        los_rios, los_lagos, aysen, magallanes
    )
    SELECT
        s.fecha,
        s.arica_parinacota, s.tarapaca, s.antofagasta, s.atacama,
        s.coquimbo, s.valparaiso, s.metropolitana, s.ohiggins,
        s.maule, s.nuble, s.bio_bio, s.araucania,
        s.los_rios, s.los_lagos, s.aysen, s.magallanes
    FROM {SILVER_SCHEMA}.{SILVER_TABLE} s
    WHERE NOT EXISTS (
        SELECT 1
        FROM {GOLD_SCHEMA}.{GOLD_TABLE} g
        WHERE g.fecha = s.fecha
    );
"""

cursor.execute(insert_sql)
print("✅ INSERT de nuevos registros regionales completado.")


# ======================================================
# 3️⃣ UPDATE SOLO SI CAMBIA ALGUNA REGIÓN
# ======================================================

update_sql = f"""
    UPDATE {GOLD_SCHEMA}.{GOLD_TABLE} g
    SET
        arica_parinacota = s.arica_parinacota,
        tarapaca = s.tarapaca,
        antofagasta = s.antofagasta,
        atacama = s.atacama,
        coquimbo = s.coquimbo,
        valparaiso = s.valparaiso,
        metropolitana = s.metropolitana,
        ohiggins = s.ohiggins,
        maule = s.maule,
        nuble = s.nuble,
        bio_bio = s.bio_bio,
        araucania = s.araucania,
        los_rios = s.los_rios,
        los_lagos = s.los_lagos,
        aysen = s.aysen,
        magallanes = s.magallanes,
        fecha_carga = CURRENT_TIMESTAMP
    FROM {SILVER_SCHEMA}.{SILVER_TABLE} s
    WHERE g.fecha = s.fecha
    AND (
        g.arica_parinacota IS DISTINCT FROM s.arica_parinacota OR
        g.tarapaca IS DISTINCT FROM s.tarapaca OR
        g.antofagasta IS DISTINCT FROM s.antofagasta OR
        g.atacama IS DISTINCT FROM s.atacama OR
        g.coquimbo IS DISTINCT FROM s.coquimbo OR
        g.valparaiso IS DISTINCT FROM s.valparaiso OR
        g.metropolitana IS DISTINCT FROM s.metropolitana OR
        g.ohiggins IS DISTINCT FROM s.ohiggins OR
        g.maule IS DISTINCT FROM s.maule OR
        g.nuble IS DISTINCT FROM s.nuble OR
        g.bio_bio IS DISTINCT FROM s.bio_bio OR
        g.araucania IS DISTINCT FROM s.araucania OR
        g.los_rios IS DISTINCT FROM s.los_rios OR
        g.los_lagos IS DISTINCT FROM s.los_lagos OR
        g.aysen IS DISTINCT FROM s.aysen OR
        g.magallanes IS DISTINCT FROM s.magallanes
    );
"""

cursor.execute(update_sql)
print("🔄 UPDATE de registros regionales modificados completado.")


# ======================================================
# 4️⃣ DELETE REGISTROS OBSOLETOS
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
print("🗑️ Eliminación de registros regionales obsoletos completada.")


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

print("\n📊 Estado final GOLD REGIONAL:")
print(f"Total filas: {result[0]}")
print(f"Desde: {result[1]}")
print(f"Hasta: {result[2]}")

cursor.close()
conn.close()

print("\n🚀 Sincronización incremental Silver → Gold regional completada correctamente.")
