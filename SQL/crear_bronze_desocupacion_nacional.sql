CREATE TABLE IF NOT EXISTS bronze_desocupacion_nacional (
    fecha DATE NOT NULL,
    desocupacion_nacional DECIMAL NOT NULL
);

CREATE TABLE IF NOT EXISTS silver_desocupacion_nacional (
    fecha DATE NOT NULL,
    desocupacion_nacional DECIMAL NOT NULL
);

CREATE TABLE IF NOT EXISTS gold_desocupacion_nacional (
    id SERIAL PRIMARY KEY,
    fecha DATE UNIQUE NOT NULL,
    desocupacion_nacional DECIMAL NOT NULL,
    fecha_carga TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

