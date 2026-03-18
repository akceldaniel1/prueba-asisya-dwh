-- ============================================================
-- ENTREGABLE 11 – SCRIPTS SQL ADICIONALES PARA EL ETL
-- Fase 3 – Pipeline ETL con Apache Airflow
-- ============================================================
-- Contiene queries de apoyo al DAG:
--   1. Verificación post-carga
--   2. Re-procesamiento y rollback por batch
--   3. Detección de duplicados entre archivos
--   4. Lookup de surrogate keys (referencia)
--   5. Limpieza manual de staging
-- ============================================================


-- ============================================================
-- SECCIÓN 1: VERIFICACIÓN POST-CARGA
-- Ejecutar después de cada corrida del DAG para validar
-- que los datos llegaron correctamente al DWH
-- ============================================================

-- 1.1 Resumen general de carga por batch
SELECT
    fv.id_batch,
    COUNT(*)                        AS total_ventas,
    SUM(fv.monto_total)             AS monto_total_cargado,
    MIN(df.fecha)                   AS fecha_min,
    MAX(df.fecha)                   AS fecha_max,
    MAX(fv.fecha_carga)             AS ultima_carga
FROM dwh.fact_ventas fv
JOIN dwh.dim_fecha   df ON df.fecha_key = fv.fecha_key
GROUP BY fv.id_batch
ORDER BY ultima_carga DESC;


-- 1.2 Comparar registros staging vs fact_ventas (por batch)
-- Usar para detectar pérdida de registros entre etapas
SELECT
    'staging_clean'                 AS origen,
    COUNT(*)                        AS total_registros,
    SUM(monto_total)                AS monto_total
FROM staging.ventas_clean
WHERE id_batch = :'batch_id'   -- pasar batch_id como parámetro

UNION ALL

SELECT
    'fact_ventas'                   AS origen,
    COUNT(*)                        AS total_registros,
    SUM(monto_total)                AS monto_total
FROM dwh.fact_ventas
WHERE id_batch = :'batch_id';


-- 1.3 Validar integridad referencial post-carga
-- Debe retornar 0 filas en todos los casos
SELECT 'fact sin dim_fecha' AS problema, COUNT(*) AS filas
FROM dwh.fact_ventas fv
LEFT JOIN dwh.dim_fecha df ON df.fecha_key = fv.fecha_key
WHERE df.fecha_key IS NULL

UNION ALL

SELECT 'fact sin dim_cliente', COUNT(*)
FROM dwh.fact_ventas fv
LEFT JOIN dwh.dim_cliente dc ON dc.cliente_key = fv.cliente_key
WHERE dc.cliente_key IS NULL

UNION ALL

SELECT 'fact sin dim_producto', COUNT(*)
FROM dwh.fact_ventas fv
LEFT JOIN dwh.dim_producto dp ON dp.producto_key = fv.producto_key
WHERE dp.producto_key IS NULL

UNION ALL

SELECT 'fact sin dim_vendedor', COUNT(*)
FROM dwh.fact_ventas fv
LEFT JOIN dwh.dim_vendedor dv ON dv.vendedor_key = fv.vendedor_key
WHERE dv.vendedor_key IS NULL;


-- 1.4 Verificar SCD Type 2 en dim_cliente
-- Cada cliente debe tener exactamente UN registro activo
SELECT
    id_cliente,
    COUNT(*) AS total_versiones,
    SUM(CASE WHEN es_registro_actual THEN 1 ELSE 0 END) AS versiones_activas
FROM dwh.dim_cliente
GROUP BY id_cliente
HAVING SUM(CASE WHEN es_registro_actual THEN 1 ELSE 0 END) <> 1
ORDER BY id_cliente;
-- Si retorna filas → problema en la lógica SCD


-- ============================================================
-- SECCIÓN 2: ROLLBACK POR BATCH
-- Usar si una carga falló a medias y se necesita revertir
-- ============================================================

-- 2.1 Ver qué batches hay cargados
SELECT DISTINCT id_batch, MIN(fecha_carga) AS fecha_carga
FROM dwh.fact_ventas
GROUP BY id_batch
ORDER BY fecha_carga DESC;

-- 2.2 Rollback completo de un batch específico
-- PRECAUCIÓN: esto elimina permanentemente los datos del batch
BEGIN;

    -- Eliminar hechos del batch
    DELETE FROM dwh.fact_ventas
    WHERE id_batch = :'batch_id';

    -- Revertir clientes creados en ese batch (SCD2)
    -- Restaurar registros cerrados por este batch
    UPDATE dwh.dim_cliente
    SET fecha_vigencia_hasta = NULL,
        es_registro_actual   = TRUE,
        fecha_actualizacion  = CURRENT_TIMESTAMP
    WHERE fecha_vigencia_hasta = CURRENT_DATE - INTERVAL '1 day'
      AND es_registro_actual = FALSE;

    -- Eliminar registros nuevos creados en ese batch
    DELETE FROM dwh.dim_cliente
    WHERE fecha_vigencia_desde = CURRENT_DATE
      AND es_registro_actual = TRUE;

    -- Eliminar staging del batch
    DELETE FROM staging.ventas_raw   WHERE id_batch = :'batch_id';
    DELETE FROM staging.ventas_clean WHERE id_batch = :'batch_id';

    -- Actualizar log de control
    UPDATE control.etl_execution_log
    SET estado = 'REVERTIDO',
        mensaje_error = 'Rollback manual ejecutado'
    WHERE id_batch = :'batch_id';

COMMIT;
-- Si algo falla: ROLLBACK;


-- ============================================================
-- SECCIÓN 3: DETECCIÓN DE DUPLICADOS
-- Queries para identificar registros problemáticos antes
-- de la carga o durante auditorías
-- ============================================================

-- 3.1 Duplicados internos en staging.ventas_raw
SELECT
    id_venta,
    COUNT(*) AS ocurrencias
FROM staging.ventas_raw
GROUP BY id_venta
HAVING COUNT(*) > 1
ORDER BY ocurrencias DESC;


-- 3.2 IDs en ventas_nuevas.csv que ya están en fact_ventas
-- Simula la detección de duplicados en carga incremental
SELECT
    sc.id_venta,
    sc.fecha,
    fv.fecha_carga AS fecha_carga_original,
    'DUPLICADO_INCREMENTAL' AS tipo_problema
FROM staging.ventas_clean    sc
JOIN dwh.fact_ventas          fv ON fv.id_venta = sc.id_venta
WHERE sc.id_batch = :'batch_id';


-- 3.3 Detectar ventas con monto_total incorrecto
-- Validación de consistencia post-carga
SELECT
    id_venta,
    cantidad,
    precio_unitario,
    monto_total,
    (cantidad * precio_unitario)    AS monto_calculado,
    ABS(monto_total - (cantidad * precio_unitario)) AS diferencia
FROM dwh.fact_ventas
WHERE ABS(monto_total - (cantidad * precio_unitario)) > 0.01;
-- Debe retornar 0 filas si el ETL calculó correctamente


-- ============================================================
-- SECCIÓN 4: LOOKUPS DE SURROGATE KEYS (referencia del DAG)
-- Estas queries las ejecuta el DAG internamente
-- Se incluyen aquí para referencia y debugging
-- ============================================================

-- 4.1 Lookup fecha_key
SELECT fecha_key
FROM dwh.dim_fecha
WHERE fecha = '2024-01-05'::DATE;
-- Resultado esperado: 20240105

-- 4.2 Lookup cliente_key (registro activo)
SELECT cliente_key
FROM dwh.dim_cliente
WHERE id_cliente = 2001
  AND es_registro_actual = TRUE;

-- 4.3 Lookup producto_key
SELECT producto_key
FROM dwh.dim_producto
WHERE id_producto = 501;

-- 4.4 Lookup vendedor_key
SELECT vendedor_key
FROM dwh.dim_vendedor
WHERE nombre_vendedor = 'Juan Pérez';

-- 4.5 Lookup canal_key
SELECT canal_key
FROM dwh.dim_canal
WHERE nombre_canal = 'Online';

-- 4.6 Lookup ciudad_key
SELECT ciudad_key
FROM dwh.dim_ciudad
WHERE nombre_ciudad = 'Bogotá';


-- ============================================================
-- SECCIÓN 5: LIMPIEZA MANUAL DE STAGING
-- Para ejecutar si la limpieza automática falla
-- ============================================================

-- 5.1 Ver cuánto ocupa staging actualmente
SELECT
    'ventas_raw'   AS tabla, COUNT(*) AS registros,
    MIN(fecha_carga) AS mas_antiguo, MAX(fecha_carga) AS mas_reciente
FROM staging.ventas_raw
UNION ALL
SELECT
    'ventas_clean', COUNT(*),
    MIN(fecha_procesamiento), MAX(fecha_procesamiento)
FROM staging.ventas_clean;

-- 5.2 Limpieza manual con política de 7 días
DELETE FROM staging.ventas_raw
WHERE fecha_carga < NOW() - INTERVAL '7 days';

DELETE FROM staging.ventas_clean
WHERE fecha_procesamiento < NOW() - INTERVAL '7 days';

-- 5.3 Ver log de calidad del último batch
SELECT
    regla_calidad,
    registros_evaluados,
    registros_validos,
    registros_invalidos,
    porcentaje_calidad,
    detalles
FROM control.data_quality_log
ORDER BY fecha_validacion DESC
LIMIT 20;
