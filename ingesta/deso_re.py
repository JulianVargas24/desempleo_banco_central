import bcchapi
from utils.conexion_postgre import get_engine
from utils.funciones import truncate_table


def run_bronze_deso_re():
    # Incluyendo credenciales explícitamente
    siete = bcchapi.Siete(file="credenciales.txt")

    series_code = [
        "F049.DES.TAS.INE9.25.M",
        "F049.DES.TAS.INE9.11.M",
        "F049.DES.TAS.INE9.12.M",
        "F049.DES.TAS.INE9.13.M",
        "F049.DES.TAS.INE9.14.M",
        "F049.DES.TAS.INE9.15.M",
        "F049.DES.TAS.INE9.23.M",
        "F049.DES.TAS.INE9.16.M",
        "F049.DES.TAS.INE9.17.M",
        "F049.DES.TAS.INE9.26.M",
        "F049.DES.TAS.INE9.18N.M",
        "F049.DES.TAS.INE9.19.M",
        "F049.DES.TAS.INE9.24.M",
        "F049.DES.TAS.INE9.20.M",
        "F049.DES.TAS.INE9.21.M",
        "F049.DES.TAS.INE9.22.M",
    ]

    df = siete.cuadro(
        series=series_code,
        nombres=[
            "arica_parinacota",
            "tarapaca",
            "antofagasta",
            "atacama",
            "coquimbo",
            "valparaiso",
            "metropolitana",
            "ohiggins",
            "maule",
            "nuble",
            "bio_bio",
            "araucania",
            "los_rios",
            "los_lagos",
            "aysen",
            "magallanes",
        ],
        desde="2015-01-01",
        frecuencia="ME",
        observado={
            "arica_parinacota": "last",
            "tarapaca": "last",
            "antofagasta": "last",
            "atacama": "last",
            "coquimbo": "last",
            "valparaiso": "last",
            "metropolitana": "last",
            "ohiggins": "last",
            "maule": "last",
            "nuble": "last",
            "bio_bio": "last",
            "araucania": "last",
            "los_rios": "last",
            "los_lagos": "last",
            "aysen": "last",
            "magallanes": "last",
        },
    )

    # Reset index para tener fecha como columna
    df = df.reset_index()
    df.columns = [
        "fecha",
        "arica_parinacota",
        "tarapaca",
        "antofagasta",
        "atacama",
        "coquimbo",
        "valparaiso",
        "metropolitana",
        "ohiggins",
        "maule",
        "nuble",
        "bio_bio",
        "araucania",
        "los_rios",
        "los_lagos",
        "aysen",
        "magallanes",
    ]

    # Conexión postgresql
    engine = get_engine()

    # Borrar datos existentes de la tabla
    truncate_table(engine, "bronze", "bronze_desocupacion_regional")

    # Ingestar postgresql
    df.to_sql(
        name="bronze_desocupacion_regional",
        schema="bronze",
        con=engine,
        if_exists="append",
        index=False,
    )

    print("✅ Bronze desocupación regional cargado correctamente")
