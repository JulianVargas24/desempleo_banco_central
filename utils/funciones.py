from sqlalchemy import text
from utils.conexion_postgre import get_engine


# FUNCION BORRA TODO LO QUE HAY EN LA TABLA en sql
def truncate_table(engine, schema, tabla):
    query = f"TRUNCATE TABLE {schema}.{tabla} RESTART IDENTITY"
    with engine.begin() as conn:
        conn.execute(text(query))


# FUNCION HACE EL TRABAJO INCREMENTAL, BORRA Y EDITA
def sync_gold(
    silver_schema: str,
    silver_table: str,
    gold_schema: str,
    gold_table: str,
    columns: list[str],
):
    """
    Sincroniza tabla Silver → Gold con lógica incremental:
    - Insert nuevos
    - Update solo si cambia
    - Delete obsoletos
    """

    engine = get_engine()
    conn = engine.raw_connection()
    cursor = conn.cursor()

    # ======================================================
    # 1️⃣ VALIDAR SILVER
    # ======================================================

    cursor.execute(f"""
        SELECT COUNT(*)
        FROM {silver_schema}.{silver_table};
    """)

    silver_count = cursor.fetchone()[0]

    if silver_count == 0:
        raise ValueError(f"❌ {silver_table} está vacío. No se puede sincronizar.")

    print(f"\n🔎 Filas en {silver_table}: {silver_count}")

    # ======================================================
    # UPSERT PROFESIONAL (INSERT + UPDATE en uno solo)
    # ======================================================

    col_list = ", ".join(columns)
    update_set = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns])

    upsert_sql = f"""
        INSERT INTO {gold_schema}.{gold_table}
            (fecha, {col_list})
        SELECT
            s.fecha,
            {col_list}
        FROM {silver_schema}.{silver_table} s
        ON CONFLICT (fecha)
        DO UPDATE SET
            {update_set},
            fecha_carga = CURRENT_TIMESTAMP;
    """

    cursor.execute(upsert_sql)

    # ======================================================
    # 4️⃣ DELETE OBSOLETOS
    # ======================================================

    delete_sql = f"""
        DELETE FROM {gold_schema}.{gold_table} g
        WHERE NOT EXISTS (
            SELECT 1
            FROM {silver_schema}.{silver_table} s
            WHERE s.fecha = g.fecha
        );
    """

    cursor.execute(delete_sql)

    # ======================================================
    # 5️⃣ COMMIT
    # ======================================================

    conn.commit()

    # ======================================================
    # 6️⃣ VALIDACIÓN FINAL
    # ======================================================

    cursor.execute(f"""
        SELECT COUNT(*), MIN(fecha), MAX(fecha)
        FROM {gold_schema}.{gold_table};
    """)

    result = cursor.fetchone()

    print("📊 Estado final GOLD:")
    print(f"Total filas: {result[0]}")
    print(f"Desde: {result[1]}")
    print(f"Hasta: {result[2]}")

    cursor.close()
    conn.close()
