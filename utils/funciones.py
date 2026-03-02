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
    Sincroniza tabla Silver → Gold con lógica incremental REAL:
    - Insert nuevos
    - Update solo si cambian valores
    - Delete obsoletos
    """

    from utils.conexion_postgre import get_engine

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
    # 2️⃣ UPSERT INTELIGENTE (INSERT + UPDATE CONDICIONAL)
    # ======================================================

    col_list = ", ".join(columns)
    select_cols = ", ".join([f"s.{col}" for col in columns])
    update_set = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns])

    distinct_conditions = " OR ".join(
        [f"{gold_table}.{col} IS DISTINCT FROM EXCLUDED.{col}" for col in columns]
    )

    upsert_sql = f"""
        INSERT INTO {gold_schema}.{gold_table}
            (fecha, {col_list})
        SELECT
            s.fecha,
            {select_cols}
        FROM {silver_schema}.{silver_table} s
        ON CONFLICT (fecha)
        DO UPDATE SET
            {update_set},
            fecha_carga = CURRENT_TIMESTAMP
        WHERE
            {distinct_conditions};
    """

    cursor.execute(upsert_sql)

    # ======================================================
    # 3️⃣ DELETE OBSOLETOS
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
    # 4️⃣ COMMIT
    # ======================================================

    conn.commit()

    # ======================================================
    # 5️⃣ VALIDACIÓN FINAL
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
