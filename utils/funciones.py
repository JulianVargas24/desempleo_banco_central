from sqlalchemy import text

def truncate_table(engine, schema, tabla):
    query = f"TRUNCATE TABLE {schema}.{tabla} RESTART IDENTITY"
    with engine.begin() as conn:
        conn.execute(text(query))