-- ============================================================
-- ENTREGABLE 8 – POBLACIÓN DE DIMENSIONES ESTÁTICAS
-- Fase 2 – Diseño del Data Warehouse
--
-- Este script pobla las dimensiones que no dependen del ETL:
--   1. dim_fecha     → rango 2020-2030 con festivos colombianos
--   2. dim_canal     → 3 canales definidos por el negocio
--   3. dim_ciudad    → ciudades presentes en los datos
--   4. dim_producto  → catálogo inicial de productos (ficticios)
--   5. dim_vendedor  → vendedores conocidos + SIN_ASIGNAR
--
-- ============================================================

INSERT INTO dwh.dim_fecha (
    fecha_key,
    fecha,
    anio,
    mes,
    dia,
    nombre_mes,
    trimestre,
    semana_anio,
    dia_semana,
    nombre_dia_semana,
    es_fin_semana,
    es_festivo,
    nombre_festivo
)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER         AS fecha_key,
    d::DATE                                  AS fecha,
    EXTRACT(YEAR    FROM d)::SMALLINT        AS anio,
    EXTRACT(MONTH   FROM d)::SMALLINT        AS mes,
    EXTRACT(DAY     FROM d)::SMALLINT        AS dia,
    TO_CHAR(d, 'TMMonth')                    AS nombre_mes,
    EXTRACT(QUARTER FROM d)::SMALLINT        AS trimestre,
    EXTRACT(WEEK    FROM d)::SMALLINT        AS semana_anio,
    EXTRACT(ISODOW  FROM d)::SMALLINT        AS dia_semana,      -- 1=Lunes, 7=Domingo
    TO_CHAR(d, 'TMDay')                      AS nombre_dia_semana,
    EXTRACT(ISODOW  FROM d) >= 6             AS es_fin_semana,   -- Sáb y Dom
    FALSE                                    AS es_festivo,
    NULL                                     AS nombre_festivo
FROM generate_series(
    '2020-01-01'::DATE,
    '2030-12-31'::DATE,
    '1 day'::INTERVAL
) AS d
ON CONFLICT (fecha) DO NOTHING;

-- Verificación
-- SELECT COUNT(*) FROM dwh.dim_fecha;  -- Debe retornar 4018

-- ============================================================
-- 1.1 FESTIVOS COLOMBIANOS FIJOS
-- Se actualizan año por año en el rango 2020-2030
-- ============================================================

DO $$
DECLARE
    y INTEGER;
BEGIN
    FOR y IN 2020..2030 LOOP

        -- 1 enero – Año Nuevo
        UPDATE dwh.dim_fecha
        SET es_festivo = TRUE, nombre_festivo = 'Año Nuevo'
        WHERE fecha = make_date(y, 1, 1);

        -- 6 enero – Reyes Magos (se traslada al lunes siguiente si no es lunes)
        UPDATE dwh.dim_fecha
        SET es_festivo = TRUE, nombre_festivo = 'Reyes Magos'
        WHERE fecha = (
            CASE WHEN EXTRACT(ISODOW FROM make_date(y,1,6)) = 1
                 THEN make_date(y,1,6)
                 ELSE make_date(y,1,6) + (8 - EXTRACT(ISODOW FROM make_date(y,1,6)))::INTEGER
            END
        );

        -- 19 marzo – San José (trasladable al lunes)
        UPDATE dwh.dim_fecha
        SET es_festivo = TRUE, nombre_festivo = 'San José'
        WHERE fecha = (
            CASE WHEN EXTRACT(ISODOW FROM make_date(y,3,19)) = 1
                 THEN make_date(y,3,19)
                 ELSE make_date(y,3,19) + (8 - EXTRACT(ISODOW FROM make_date(y,3,19)))::INTEGER
            END
        );

        -- 1 mayo – Día del Trabajo (fijo)
        UPDATE dwh.dim_fecha
        SET es_festivo = TRUE, nombre_festivo = 'Día del Trabajo'
        WHERE fecha = make_date(y, 5, 1);

        -- 29 junio – San Pedro y San Pablo (trasladable)
        UPDATE dwh.dim_fecha
        SET es_festivo = TRUE, nombre_festivo = 'San Pedro y San Pablo'
        WHERE fecha = (
            CASE WHEN EXTRACT(ISODOW FROM make_date(y,6,29)) = 1
                 THEN make_date(y,6,29)
                 ELSE make_date(y,6,29) + (8 - EXTRACT(ISODOW FROM make_date(y,6,29)))::INTEGER
            END
        );

        -- 20 julio – Independencia de Colombia (fijo)
        UPDATE dwh.dim_fecha
        SET es_festivo = TRUE, nombre_festivo = 'Independencia de Colombia'
        WHERE fecha = make_date(y, 7, 20);

        -- 7 agosto – Batalla de Boyacá (fijo)
        UPDATE dwh.dim_fecha
        SET es_festivo = TRUE, nombre_festivo = 'Batalla de Boyacá'
        WHERE fecha = make_date(y, 8, 7);

        -- 15 agosto – Asunción de la Virgen (trasladable)
        UPDATE dwh.dim_fecha
        SET es_festivo = TRUE, nombre_festivo = 'Asunción de la Virgen'
        WHERE fecha = (
            CASE WHEN EXTRACT(ISODOW FROM make_date(y,8,15)) = 1
                 THEN make_date(y,8,15)
                 ELSE make_date(y,8,15) + (8 - EXTRACT(ISODOW FROM make_date(y,8,15)))::INTEGER
            END
        );

        -- 12 octubre – Día de la Raza (trasladable)
        UPDATE dwh.dim_fecha
        SET es_festivo = TRUE, nombre_festivo = 'Día de la Raza'
        WHERE fecha = (
            CASE WHEN EXTRACT(ISODOW FROM make_date(y,10,12)) = 1
                 THEN make_date(y,10,12)
                 ELSE make_date(y,10,12) + (8 - EXTRACT(ISODOW FROM make_date(y,10,12)))::INTEGER
            END
        );

        -- 1 noviembre – Todos los Santos (trasladable)
        UPDATE dwh.dim_fecha
        SET es_festivo = TRUE, nombre_festivo = 'Todos los Santos'
        WHERE fecha = (
            CASE WHEN EXTRACT(ISODOW FROM make_date(y,11,1)) = 1
                 THEN make_date(y,11,1)
                 ELSE make_date(y,11,1) + (8 - EXTRACT(ISODOW FROM make_date(y,11,1)))::INTEGER
            END
        );

        -- 11 noviembre – Independencia de Cartagena (trasladable)
        UPDATE dwh.dim_fecha
        SET es_festivo = TRUE, nombre_festivo = 'Independencia de Cartagena'
        WHERE fecha = (
            CASE WHEN EXTRACT(ISODOW FROM make_date(y,11,11)) = 1
                 THEN make_date(y,11,11)
                 ELSE make_date(y,11,11) + (8 - EXTRACT(ISODOW FROM make_date(y,11,11)))::INTEGER
            END
        );

        -- 8 diciembre – Inmaculada Concepción (fijo)
        UPDATE dwh.dim_fecha
        SET es_festivo = TRUE, nombre_festivo = 'Inmaculada Concepción'
        WHERE fecha = make_date(y, 12, 8);

        -- 25 diciembre – Navidad (fijo)
        UPDATE dwh.dim_fecha
        SET es_festivo = TRUE, nombre_festivo = 'Navidad'
        WHERE fecha = make_date(y, 12, 25);

    END LOOP;
END $$;

-- Verificación de festivos cargados
-- SELECT fecha, nombre_festivo FROM dwh.dim_fecha
-- WHERE es_festivo = TRUE ORDER BY fecha;

-- ============================================================
-- 2. POBLAR dim_canal
-- ============================================================

INSERT INTO dwh.dim_canal (nombre_canal, tipo_canal, descripcion, comision_pct)
VALUES
    ('Online',      'Digital', 'Ventas directas por plataforma web o app propia',          0.00),
    ('Tienda',      'Físico',  'Ventas en punto de venta físico',                           0.00),
    ('Marketplace', 'Digital', 'Ventas a través de plataformas terceras (MercadoLibre etc)', 5.00)
ON CONFLICT (nombre_canal) DO NOTHING;

-- ============================================================
-- 3. POBLAR dim_ciudad
-- ============================================================

INSERT INTO dwh.dim_ciudad (nombre_ciudad, departamento, region, poblacion, latitud, longitud)
VALUES
    ('Bogotá',       'Cundinamarca', 'Andina',    8380000,  4.710989, -74.072092),
    ('Medellín',     'Antioquia',    'Andina',    2700000,  6.244203, -75.581211),
    ('Cali',         'Valle',        'Pacífica',  2300000,  3.451647, -76.531985),
    ('Barranquilla', 'Atlántico',    'Caribe',    1300000, 10.963889, -74.796387),
    ('Cartagena',    'Bolívar',      'Caribe',    1000000, 10.391049, -75.479426),
    ('Bucaramanga',  'Santander',    'Andina',     600000,  7.119349, -73.122742),
    ('Pereira',      'Risaralda',    'Andina',     490000,  4.813333, -75.696111),
    ('Manizales',    'Caldas',       'Andina',     430000,  5.070000, -75.513056)
ON CONFLICT (nombre_ciudad) DO NOTHING;

-- ============================================================
-- 4. POBLAR dim_producto (catálogo ficticio)
-- ============================================================

INSERT INTO dwh.dim_producto
    (id_producto, nombre_producto, categoria, subcategoria, precio_referencia, unidad_medida)
VALUES
    (501, 'Audífonos Bluetooth Pro',       'Electrónica', 'Audio',              35000, 'Unidad'),
    (502, 'Silla Ergonómica Plus',         'Hogar',       'Muebles',            45000, 'Unidad'),
    (503, 'Camiseta Deportiva Dry-Fit',    'Ropa',        'Deportiva',          15000, 'Unidad'),
    (504, 'Licuadora 600W',               'Hogar',       'Electrodomésticos',  22000, 'Unidad'),
    (505, 'Mochila Impermeable 30L',       'Accesorios',  'Bolsos',             25000, 'Unidad'),
    (506, 'Monitor LED 24"',              'Electrónica', 'Monitores',          55000, 'Unidad'),
    (507, 'Teclado Mecánico RGB',          'Electrónica', 'Periféricos',        80000, 'Unidad'),
    (508, 'Lámpara LED Escritorio',        'Hogar',       'Iluminación',        30000, 'Unidad'),
    (509, 'Maletín Ejecutivo',             'Accesorios',  'Bolsos',             20000, 'Unidad'),
    (510, 'Zapatillas Running',            'Ropa',        'Calzado',            12000, 'Par'),
    (511, 'Smartwatch Fitness',            'Electrónica', 'Wearables',          95000, 'Unidad'),
    (512, 'Cafetera Espresso Manual',      'Hogar',       'Electrodomésticos',  NULL,  'Unidad')
ON CONFLICT (id_producto) DO NOTHING;

-- ============================================================
-- 5. POBLAR dim_vendedor (vendedores conocidos del dataset)
-- ============================================================

INSERT INTO dwh.dim_vendedor (nombre_vendedor, ciudad_base, region, zona, estado)
VALUES
    ('SIN_ASIGNAR',  NULL,          NULL,       NULL,       'Activo'),
    ('Juan Pérez',   'Bogotá',      'Andina',   'Centro',   'Activo'),
    ('Ana Torres',   'Bogotá',      'Andina',   'Centro',   'Activo'),
    ('Laura Díaz',   'Cali',        'Pacífica', 'Sur',      'Activo'),
    ('Camilo Rojas', 'Medellín',    'Andina',   'Norte',    'Activo'),
    ('Roberto Vega', 'Barranquilla','Caribe',   'Costa',    'Activo')
ON CONFLICT (nombre_vendedor) DO NOTHING;

-- ============================================================
-- VERIFICACIONES FINALES
-- ============================================================

-- Descomenta para verificar después de ejecutar:

-- SELECT 'dim_fecha'    AS tabla, COUNT(*) AS registros FROM dwh.dim_fecha    UNION ALL
-- SELECT 'dim_canal'    AS tabla, COUNT(*) AS registros FROM dwh.dim_canal    UNION ALL
-- SELECT 'dim_ciudad'   AS tabla, COUNT(*) AS registros FROM dwh.dim_ciudad   UNION ALL
-- SELECT 'dim_producto' AS tabla, COUNT(*) AS registros FROM dwh.dim_producto UNION ALL
-- SELECT 'dim_vendedor' AS tabla, COUNT(*) AS registros FROM dwh.dim_vendedor;

-- Resultado esperado:
-- dim_fecha    → 4018
-- dim_canal    → 3
-- dim_ciudad   → 8
-- dim_producto → 12
-- dim_vendedor → 6 (incluye SIN_ASIGNAR)

-- Festivos por año (debe haber ~18 por año):
-- SELECT anio, COUNT(*) FROM dwh.dim_fecha
-- WHERE es_festivo = TRUE GROUP BY anio ORDER BY anio;
