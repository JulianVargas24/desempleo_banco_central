CREATE TABLE IF NOT EXISTS bronze_fuerza_trabajo (
    fecha DATE NOT NULL,
    total DECIMAL NOT NULL,
	mujeres DECIMAL NOT NULL,
	hombres DECIMAL NOT NULL 
	);

CREATE TABLE IF NOT EXISTS silver_fuerza_trabajo (
    fecha DATE NOT NULL,
    total DECIMAL NOT NULL,
	mujeres DECIMAL NOT NULL,
	hombres DECIMAL NOT NULL 
	);

CREATE TABLE IF NOT EXISTS gold_fuerza_trabajo (
    id SERIAL PRIMARY KEY,
    fecha DATE UNIQUE NOT NULL,
	fecha_carga TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    total DECIMAL NOT NULL,
	mujeres DECIMAL NOT NULL,
	hombres DECIMAL NOT NULL 
	);