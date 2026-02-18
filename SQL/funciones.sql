UPDATE silver.silver_desocupacion_nacional
SET desocupacion_nacional = 6.66
WHERE fecha = '2026-01-31';

DELETE FROM gold.gold_desocupacion_nacional
WHERE fecha = '2026-01-31';

INSERT INTO silver.silver_desocupacion_nacional (fecha, desocupacion_nacional)
VALUES ('2026-01-31', 9.99)