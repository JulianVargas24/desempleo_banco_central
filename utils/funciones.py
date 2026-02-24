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
    # 2️⃣ INSERT NUEVOS
    # ======================================================

    col_list = ", ".join(columns)
    select_cols = ", ".join([f"s.{col}" for col in columns])

    insert_sql = f"""
        INSERT INTO {gold_schema}.{gold_table}
            (fecha, {col_list})
        SELECT
            s.fecha,
            {select_cols}
        FROM {silver_schema}.{silver_table} s
        WHERE NOT EXISTS (
            SELECT 1
            FROM {gold_schema}.{gold_table} g
            WHERE g.fecha = s.fecha
        );
    """

    cursor.execute(insert_sql)

    # ======================================================
    # 3️⃣ UPDATE SI CAMBIA
    # ======================================================

    set_clause = ",\n        ".join([f"{col} = s.{col}" for col in columns])

    distinct_conditions = " OR\n        ".join(
        [f"g.{col} IS DISTINCT FROM s.{col}" for col in columns]
    )

    update_sql = f"""
        UPDATE {gold_schema}.{gold_table} g
        SET
            {set_clause},
            fecha_carga = CURRENT_TIMESTAMP
        FROM {silver_schema}.{silver_table} s
        WHERE g.fecha = s.fecha
        AND (
            {distinct_conditions}
        );
    """

    cursor.execute(update_sql)

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
