import bcchapi
from utils.conexion_postgre import get_engine
from utils.funciones import truncate_table


def run_bronze_deso_na():
    # Consumo API Banco Central
    siete = bcchapi.Siete(file="/opt/airflow/project/credenciales.txt")

    series_code = [
        "F049.DES.TAS.INE9.10.M",
        "F049.DES.PMT.INE.03.M",
        "F049.DES.PMT.INE.02.M",
    ]

    df = siete.cuadro(
        series=series_code,
        nombres=[
            "desocupacion_nacional",
            "desocupacion_mujeres",
            "desocupacion_hombres",
        ],
        desde="2015-01-01",
        frecuencia="M",
        observado={
            "desocupacion_nacional": "last",
            "desocupacion_mujeres": "last",
            "desocupacion_hombres": "last",
        },
    )

    # Reset index
    df = df.reset_index()
    df.columns = [
        "fecha",
        "desocupacion_nacional",
        "desocupacion_mujeres",
        "desocupacion_hombres",
    ]

    # Eliminar duplicados por fecha
    df = df.drop_duplicates(subset=["fecha"], keep="last")

    # Conexión
    engine = get_engine()

    # Truncate Bronze
    truncate_table(engine, "bronze", "bronze_desocupacion_nacional")

    # Insertar datos
    df.to_sql(
        name="bronze_desocupacion_nacional",
        schema="bronze",
        con=engine,
        if_exists="append",
        index=False,
    )

    print("✅ Bronze desocupación nacional cargado correctamente")
