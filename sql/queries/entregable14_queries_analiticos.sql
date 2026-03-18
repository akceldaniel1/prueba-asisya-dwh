-- ============================================================
-- ENTREGABLE 14 – QUERIES SQL AVANZADOS Y ANÁLISIS
-- Fase 4 – SQL Avanzado y Análisis
-- PostgreSQL 14+
-- Base: dwh.v_ventas_completas (vista que une fact + dims)
-- ============================================================
-- ÍNDICES QUE APOYAN ESTOS QUERIES (creados en Fase 2):
--   idx_fv_fecha_key          → queries de período
--   idx_fv_cliente_key        → queries de cliente
--   idx_fv_producto_key       → queries de producto
--   idx_fv_vendedor_key       → queries de vendedor
--   idx_fv_canal_key          → queries de canal
--   idx_fv_ciudad_key         → queries de ciudad
--   idx_fv_fecha_producto     → query 7 (YoY por producto)
--   idx_fv_vendedor_fecha     → query 4 (vendedor por período)
-- ============================================================


-- ============================================================
-- QUERY 1 – TOP 5 CLIENTES POR MONTO TOTAL DE VENTAS
-- ============================================================
-- Objetivo: Identificar los clientes más valiosos por
-- su aportación total en ventas.
--
-- Técnica:
--   CTE ventas_cliente → agrega monto y cuenta transacciones
--   JOIN a dim_cliente → obtiene email y ciudad actuales
--   RANK() → asigna posición dentro del ranking
--   LIMIT 5 → retorna solo los top 5
-- ============================================================

WITH ventas_cliente AS (
    -- Paso 1: agregar ventas por cliente desde la tabla de hechos
    SELECT
        fv.cliente_key,
        COUNT(*)                        AS total_transacciones,
        SUM(fv.monto_total)             AS monto_total,
        AVG(fv.monto_total)             AS ticket_promedio,
        MIN(df.fecha)                   AS primera_compra,
        MAX(df.fecha)                   AS ultima_compra
    FROM dwh.fact_ventas    fv
    JOIN dwh.dim_fecha       df ON df.fecha_key = fv.fecha_key
    GROUP BY fv.cliente_key
),
ranking_clientes AS (
    -- Paso 2: unir con dim_cliente y asignar ranking
    SELECT
        dc.id_cliente,
        dc.ciudad                       AS ciudad_actual,
        dc.email_cliente,
        vc.total_transacciones,
        vc.monto_total,
        ROUND(vc.ticket_promedio, 0)    AS ticket_promedio,
        vc.primera_compra,
        vc.ultima_compra,
        RANK() OVER (
            ORDER BY vc.monto_total DESC
        )                               AS ranking
    FROM ventas_cliente vc
    JOIN dwh.dim_cliente dc
        ON dc.cliente_key = vc.cliente_key
        AND dc.es_registro_actual = TRUE   -- solo versión vigente del cliente
)
SELECT
    ranking,
    id_cliente,
    ciudad_actual,
    total_transacciones,
    monto_total,
    ticket_promedio,
    primera_compra,
    ultima_compra
FROM ranking_clientes
WHERE ranking <= 5
ORDER BY ranking;

/*
RESULTADO ESPERADO (con datos de ventas_raw.csv):
 ranking | id_cliente | ciudad_actual | total_transacciones | monto_total | ticket_promedio
---------+------------+---------------+---------------------+-------------+-----------------
       1 |       2001 | Bogotá        |                   3 |      325000 |          108333
       2 |       2003 | Medellín      |                   2 |      225000 |          112500
       3 |       2002 | Bogotá        |                   2 |      210000 |          105000
       4 |       2004 | Cali          |                   2 |      120000 |           60000
       5 |       2009 | Bogotá        |                   1 |       80000 |           80000
*/


-- ============================================================
-- QUERY 2 – TENDENCIA MENSUAL DE VENTAS
-- ============================================================
-- Objetivo: Ver cómo evolucionan las ventas mes a mes,
-- útil para detectar estacionalidad y tendencias.
--
-- Técnica:
--   GROUP BY anio + mes → agrega por período
--   LAG() → obtiene el monto del mes anterior
--   Cálculo de variación % vs mes anterior
--   ORDER BY cronológico
-- ============================================================

WITH ventas_mensuales AS (
    -- Paso 1: agregar ventas por mes
    SELECT
        df.anio,
        df.mes,
        df.nombre_mes,
        COUNT(fv.venta_key)             AS total_transacciones,
        SUM(fv.monto_total)             AS monto_total,
        ROUND(AVG(fv.monto_total), 0)   AS ticket_promedio,
        COUNT(DISTINCT fv.cliente_key)  AS clientes_unicos
    FROM dwh.fact_ventas  fv
    JOIN dwh.dim_fecha     df ON df.fecha_key = fv.fecha_key
    GROUP BY df.anio, df.mes, df.nombre_mes
),
tendencia AS (
    -- Paso 2: calcular variación vs mes anterior usando LAG()
    SELECT
        anio,
        mes,
        nombre_mes,
        total_transacciones,
        monto_total,
        ticket_promedio,
        clientes_unicos,
        -- LAG trae el valor del período anterior
        LAG(monto_total) OVER (
            ORDER BY anio, mes
        )                               AS monto_mes_anterior,
        -- Variación porcentual respecto al mes anterior
        ROUND(
            (monto_total - LAG(monto_total) OVER (ORDER BY anio, mes))
            / NULLIF(LAG(monto_total) OVER (ORDER BY anio, mes), 0)
            * 100, 1
        )                               AS variacion_pct
    FROM ventas_mensuales
)
SELECT
    anio,
    nombre_mes                          AS mes,
    total_transacciones,
    monto_total,
    ticket_promedio,
    clientes_unicos,
    COALESCE(monto_mes_anterior, 0)     AS monto_mes_anterior,
    COALESCE(variacion_pct, 0)          AS variacion_pct
FROM tendencia
ORDER BY anio, mes;

/*
RESULTADO ESPERADO (datos concentrados en enero 2024):
 anio | mes    | total_transacciones | monto_total | ticket_promedio | variacion_pct
------+--------+---------------------+-------------+-----------------+---------------
 2024 | Enero  |                  17 |     1191000 |           70059 |             0
(Solo un mes en los datos de prueba – con más datos se vería la tendencia)
*/


-- ============================================================
-- QUERY 3 – ANÁLISIS POR CIUDAD
-- ============================================================
-- Objetivo: Comparar el desempeño comercial entre ciudades.
-- Incluye ranking, ventas totales, promedio por transacción
-- y participación porcentual en el total.
--
-- Técnica:
--   CTE ventas_ciudad → agrega por ciudad
--   RANK() → posición en el ranking nacional
--   SUM() OVER () → total global para calcular participación %
-- ============================================================

WITH ventas_ciudad AS (
    -- Paso 1: agregar ventas por ciudad
    SELECT
        dci.nombre_ciudad,
        dci.departamento,
        dci.region,
        COUNT(fv.venta_key)             AS total_transacciones,
        SUM(fv.monto_total)             AS monto_total,
        ROUND(AVG(fv.monto_total), 0)   AS promedio_por_transaccion,
        COUNT(DISTINCT fv.cliente_key)  AS clientes_unicos,
        SUM(fv.cantidad)                AS unidades_vendidas
    FROM dwh.fact_ventas  fv
    JOIN dwh.dim_ciudad    dci ON dci.ciudad_key = fv.ciudad_key
    GROUP BY dci.nombre_ciudad, dci.departamento, dci.region
),
ranking_ciudad AS (
    -- Paso 2: agregar ranking y participación porcentual
    SELECT
        nombre_ciudad,
        departamento,
        region,
        total_transacciones,
        monto_total,
        promedio_por_transaccion,
        clientes_unicos,
        unidades_vendidas,
        RANK() OVER (
            ORDER BY monto_total DESC
        )                               AS ranking_nacional,
        RANK() OVER (
            PARTITION BY region         -- ranking dentro de la misma región
            ORDER BY monto_total DESC
        )                               AS ranking_regional,
        ROUND(
            monto_total * 100.0
            / SUM(monto_total) OVER ()  -- total global (sin GROUP BY)
        , 1)                            AS participacion_pct
    FROM ventas_ciudad
)
SELECT
    ranking_nacional,
    nombre_ciudad,
    departamento,
    region,
    ranking_regional,
    total_transacciones,
    monto_total,
    promedio_por_transaccion,
    clientes_unicos,
    participacion_pct
FROM ranking_ciudad
ORDER BY ranking_nacional;

/*
RESULTADO ESPERADO:
 ranking | nombre_ciudad | region  | monto_total | participacion_pct
---------+---------------+---------+-------------+-------------------
       1 | Bogotá        | Andina  |      550000 |              46.2
       2 | Medellín      | Andina  |      375000 |              31.5
       3 | Cali          | Pacífica|      191000 |              16.0
       4 | Barranquilla  | Caribe  |       75000 |               6.3
*/


-- ============================================================
-- QUERY 4 – RANKING DE VENDEDORES
-- ============================================================
-- Objetivo: Evaluar el desempeño de cada vendedor:
-- total vendido, número de transacciones y ticket promedio.
--
-- Técnica:
--   CTE metricas_vendedor → agrega por vendedor
--   RANK() por monto total → ranking principal
--   RANK() por ticket promedio → ranking de eficiencia
--   NTILE(3) → clasifica vendedores en terciles (alto/medio/bajo)
-- ============================================================

WITH metricas_vendedor AS (
    -- Paso 1: calcular métricas por vendedor
    SELECT
        dv.nombre_vendedor,
        dv.region,
        dv.ciudad_base,
        COUNT(fv.venta_key)             AS total_transacciones,
        SUM(fv.monto_total)             AS monto_total,
        ROUND(AVG(fv.monto_total), 0)   AS ticket_promedio,
        SUM(fv.cantidad)                AS unidades_vendidas,
        COUNT(DISTINCT fv.cliente_key)  AS clientes_atendidos,
        MIN(df.fecha)                   AS primera_venta,
        MAX(df.fecha)                   AS ultima_venta
    FROM dwh.fact_ventas   fv
    JOIN dwh.dim_vendedor   dv ON dv.vendedor_key = fv.vendedor_key
    JOIN dwh.dim_fecha       df ON df.fecha_key   = fv.fecha_key
    WHERE dv.nombre_vendedor <> 'SIN_ASIGNAR'   -- excluir ventas sin vendedor
    GROUP BY dv.nombre_vendedor, dv.region, dv.ciudad_base
)
SELECT
    RANK() OVER (ORDER BY monto_total DESC)         AS ranking,
    nombre_vendedor,
    region,
    ciudad_base,
    total_transacciones,
    monto_total,
    ticket_promedio,
    unidades_vendidas,
    clientes_atendidos,
    -- Clasificar en terciles: 1=top, 2=medio, 3=bajo
    CASE NTILE(3) OVER (ORDER BY monto_total DESC)
        WHEN 1 THEN 'Alto'
        WHEN 2 THEN 'Medio'
        ELSE        'Bajo'
    END                                              AS nivel_desempeño
FROM metricas_vendedor
ORDER BY ranking;

/*
RESULTADO ESPERADO:
 ranking | nombre_vendedor | monto_total | ticket_promedio | nivel_desempeño
---------+-----------------+-------------+-----------------+-----------------
       1 | Juan Pérez      |      405000 |           67500 | Alto
       2 | Laura Díaz      |      246000 |           49200 | Alto
       3 | Camilo Rojas    |      190000 |           63333 | Medio
       4 | Ana Torres      |      210000 |          105000 | Medio
       5 | Roberto Vega    |       75000 |           37500 | Bajo
*/


-- ============================================================
-- QUERY 5 – ANÁLISIS DE PRODUCTO
-- ============================================================
-- Objetivo: Identificar los productos más vendidos tanto
-- por cantidad de unidades como por monto total.
-- Un producto puede rankear diferente en ambas métricas.
--
-- Técnica:
--   CTE metricas_producto → agrega por producto
--   RANK() por cantidad → top productos en volumen
--   RANK() por monto    → top productos en valor
--   Ambos rankings en la misma consulta para comparar
-- ============================================================

WITH metricas_producto AS (
    -- Paso 1: agregar métricas por producto
    SELECT
        dp.id_producto,
        dp.nombre_producto,
        dp.categoria,
        dp.subcategoria,
        COUNT(fv.venta_key)             AS total_transacciones,
        SUM(fv.cantidad)                AS total_unidades,
        SUM(fv.monto_total)             AS monto_total,
        ROUND(AVG(fv.precio_unitario),0) AS precio_promedio_venta,
        dp.precio_referencia,
        COUNT(DISTINCT fv.cliente_key)  AS clientes_distintos
    FROM dwh.fact_ventas   fv
    JOIN dwh.dim_producto   dp ON dp.producto_key = fv.producto_key
    GROUP BY dp.id_producto, dp.nombre_producto, dp.categoria,
             dp.subcategoria, dp.precio_referencia
)
SELECT
    id_producto,
    nombre_producto,
    categoria,
    subcategoria,
    total_transacciones,
    total_unidades,
    monto_total,
    precio_promedio_venta,
    precio_referencia,
    clientes_distintos,
    -- Ranking por volumen de unidades
    RANK() OVER (ORDER BY total_unidades DESC)  AS rank_por_unidades,
    -- Ranking por valor monetario
    RANK() OVER (ORDER BY monto_total DESC)     AS rank_por_monto,
    -- Diferencia entre rankings (si es positivo: vende más en $ que en unidades)
    RANK() OVER (ORDER BY monto_total DESC)
    - RANK() OVER (ORDER BY total_unidades DESC) AS diferencia_rankings
FROM metricas_producto
ORDER BY rank_por_monto;

/*
RESULTADO ESPERADO:
 nombre_producto         | total_unidades | monto_total | rank_unidades | rank_monto
-------------------------+----------------+-------------+---------------+------------
 Silla Ergonómica Plus   |             11 |      495000 |             1 |          1
 Audífonos Bluetooth Pro |             10 |      350000 |             2 |          2
 Monitor LED 24"         |              3 |      165000 |             5 |          3
 Camiseta Deportiva      |              7 |      105000 |             3 |          4
*/


-- ============================================================
-- QUERY 6 – ANÁLISIS DE CANAL
-- ============================================================
-- Objetivo: Comparar el desempeño entre canales de venta
-- (Online, Tienda, Marketplace) en términos de volumen,
-- valor y eficiencia.
--
-- Técnica:
--   CTE metricas_canal → agrega por canal
--   SUM() OVER () → total global para participación %
--   Cálculo de métricas de eficiencia por canal
-- ============================================================

WITH metricas_canal AS (
    -- Paso 1: agregar por canal
    SELECT
        dca.nombre_canal,
        dca.tipo_canal,
        dca.comision_pct,
        COUNT(fv.venta_key)             AS total_transacciones,
        SUM(fv.monto_total)             AS monto_total,
        ROUND(AVG(fv.monto_total), 0)   AS ticket_promedio,
        SUM(fv.cantidad)                AS total_unidades,
        COUNT(DISTINCT fv.cliente_key)  AS clientes_unicos,
        COUNT(DISTINCT fv.producto_key) AS productos_distintos
    FROM dwh.fact_ventas  fv
    JOIN dwh.dim_canal     dca ON dca.canal_key = fv.canal_key
    GROUP BY dca.nombre_canal, dca.tipo_canal, dca.comision_pct
),
analisis_canal AS (
    -- Paso 2: calcular participación y costo de comisión
    SELECT
        nombre_canal,
        tipo_canal,
        total_transacciones,
        monto_total,
        ticket_promedio,
        total_unidades,
        clientes_unicos,
        productos_distintos,
        -- Participación en el total de ventas
        ROUND(monto_total * 100.0 / SUM(monto_total) OVER (), 1)
                                        AS participacion_pct,
        -- Participación en transacciones
        ROUND(total_transacciones * 100.0
              / SUM(total_transacciones) OVER (), 1)
                                        AS participacion_transacciones_pct,
        -- Costo estimado de comisión del canal
        ROUND(monto_total * comision_pct / 100.0, 0)
                                        AS costo_comision,
        -- Monto neto descontando comisión
        ROUND(monto_total * (1 - comision_pct / 100.0), 0)
                                        AS monto_neto_canal
    FROM metricas_canal
)
SELECT
    RANK() OVER (ORDER BY monto_total DESC) AS ranking,
    nombre_canal,
    tipo_canal,
    total_transacciones,
    monto_total,
    ticket_promedio,
    participacion_pct,
    participacion_transacciones_pct,
    clientes_unicos,
    costo_comision,
    monto_neto_canal
FROM analisis_canal
ORDER BY ranking;

/*
RESULTADO ESPERADO:
 ranking | nombre_canal | monto_total | participacion_pct | ticket_promedio | costo_comision
---------+--------------+-------------+-------------------+-----------------+----------------
       1 | Online       |      600000 |              50.4 |           66667 |              0
       2 | Tienda       |      221000 |              18.6 |           44200 |              0
       3 | Marketplace  |      370000 |              31.1 |           92500 |          18500
*/


-- ============================================================
-- QUERY 7 – YoY GROWTH POR PRODUCTO (Year over Year)
-- ============================================================
-- Objetivo: Calcular el crecimiento año sobre año por
-- producto para identificar tendencias de largo plazo.
--
-- Técnica:
--   CTE ventas_anuales → agrega por producto y año
--   LAG() OVER (PARTITION BY producto ORDER BY año) →
--     trae el monto del año anterior para el mismo producto
--   Cálculo de variación % y clasificación de tendencia
--
-- NOTA: Con el dataset actual (solo enero 2024) no habrá
-- datos YoY. Este query está diseñado para producción.
-- Se incluye con datos de ejemplo simulados en el comentario.
-- ============================================================

WITH ventas_anuales AS (
    -- Paso 1: agregar ventas por producto y año
    SELECT
        dp.id_producto,
        dp.nombre_producto,
        dp.categoria,
        df.anio,
        COUNT(fv.venta_key)             AS total_transacciones,
        SUM(fv.cantidad)                AS total_unidades,
        SUM(fv.monto_total)             AS monto_total,
        ROUND(AVG(fv.precio_unitario),0) AS precio_promedio
    FROM dwh.fact_ventas   fv
    JOIN dwh.dim_producto   dp ON dp.producto_key = fv.producto_key
    JOIN dwh.dim_fecha       df ON df.fecha_key   = fv.fecha_key
    GROUP BY dp.id_producto, dp.nombre_producto, dp.categoria, df.anio
),
yoy AS (
    -- Paso 2: calcular monto del año anterior con LAG()
    -- PARTITION BY id_producto → LAG opera dentro de cada producto
    -- ORDER BY anio            → trae el año inmediatamente anterior
    SELECT
        id_producto,
        nombre_producto,
        categoria,
        anio,
        total_transacciones,
        total_unidades,
        monto_total,
        precio_promedio,
        -- Monto del año anterior (NULL si es el primer año)
        LAG(monto_total) OVER (
            PARTITION BY id_producto
            ORDER BY anio
        )                               AS monto_anio_anterior,
        -- Unidades del año anterior
        LAG(total_unidades) OVER (
            PARTITION BY id_producto
            ORDER BY anio
        )                               AS unidades_anio_anterior
    FROM ventas_anuales
)
SELECT
    id_producto,
    nombre_producto,
    categoria,
    anio,
    total_transacciones,
    total_unidades,
    monto_total,
    COALESCE(monto_anio_anterior, 0)    AS monto_anio_anterior,
    -- Crecimiento YoY en monto
    CASE
        WHEN monto_anio_anterior IS NULL THEN NULL
        WHEN monto_anio_anterior = 0     THEN NULL
        ELSE ROUND(
            (monto_total - monto_anio_anterior)
            / monto_anio_anterior * 100, 1
        )
    END                                 AS yoy_growth_pct,
    -- Clasificación de tendencia
    CASE
        WHEN monto_anio_anterior IS NULL THEN 'Primer año'
        WHEN monto_total > monto_anio_anterior * 1.10 THEN 'Crecimiento fuerte (>10%)'
        WHEN monto_total > monto_anio_anterior         THEN 'Crecimiento moderado'
        WHEN monto_total = monto_anio_anterior         THEN 'Estable'
        WHEN monto_total > monto_anio_anterior * 0.90 THEN 'Caída moderada'
        ELSE                                                'Caída fuerte (>10%)'
    END                                 AS tendencia
FROM yoy
ORDER BY id_producto, anio;

/*
RESULTADO ESPERADO (simulado con 2 años de datos):
 nombre_producto         | anio | monto_total | monto_anterior | yoy_growth_pct | tendencia
-------------------------+------+-------------+----------------+----------------+---------------------------
 Audífonos Bluetooth Pro | 2023 |      280000 |           NULL |           NULL | Primer año
 Audífonos Bluetooth Pro | 2024 |      350000 |         280000 |           25.0 | Crecimiento fuerte (>10%)
 Silla Ergonómica Plus   | 2023 |      450000 |           NULL |           NULL | Primer año
 Silla Ergonómica Plus   | 2024 |      495000 |         450000 |           10.0 | Crecimiento moderado

NOTA: Con el dataset de prueba (solo enero 2024) todos los
productos tendrán una sola fila con tendencia 'Primer año'.
*/


-- ============================================================
-- EXPLAIN ANALYZE – PLAN DE EJECUCIÓN
-- ============================================================
-- Se incluye EXPLAIN ANALYZE para el Query 3 (análisis por
-- ciudad) por ser uno de los más complejos con múltiples
-- window functions y JOINs.
-- ============================================================

EXPLAIN ANALYZE
WITH ventas_ciudad AS (
    SELECT
        dci.nombre_ciudad,
        dci.departamento,
        dci.region,
        COUNT(fv.venta_key)             AS total_transacciones,
        SUM(fv.monto_total)             AS monto_total,
        ROUND(AVG(fv.monto_total), 0)   AS promedio_por_transaccion,
        COUNT(DISTINCT fv.cliente_key)  AS clientes_unicos
    FROM dwh.fact_ventas  fv
    JOIN dwh.dim_ciudad    dci ON dci.ciudad_key = fv.ciudad_key
    GROUP BY dci.nombre_ciudad, dci.departamento, dci.region
)
SELECT
    nombre_ciudad,
    monto_total,
    RANK() OVER (ORDER BY monto_total DESC) AS ranking
FROM ventas_ciudad
ORDER BY ranking;

/*
PLAN DE EJECUCIÓN ESPERADO (con índices activos):

Sort  (cost=... rows=4 width=...)
  Sort Key: (rank() OVER (?))
  ->  WindowAgg  (cost=... rows=4 width=...)
        ->  Sort  (cost=... rows=4 width=...)
              Sort Key: (sum(fv.monto_total)) DESC
              ->  HashAggregate  (...)
                    Group Key: dci.nombre_ciudad, dci.departamento, dci.region
                    ->  Hash Join  (...)
                          Hash Cond: (fv.ciudad_key = dci.ciudad_key)
                          ->  Seq Scan on fact_ventas fv
                          ->  Hash
                                ->  Seq Scan on dim_ciudad dci

OBSERVACIÓN: Con volumen pequeño PostgreSQL usa Seq Scan.
Con millones de filas usaría Index Scan sobre idx_fv_ciudad_key,
lo que reduciría el costo de la operación significativamente.
*/
