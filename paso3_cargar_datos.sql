-- ============================================================
-- SCRIPT DE CARGA DE DATOS – ventas_raw.csv + ventas_nuevas.csv
-- Ejecutar DESPUÉS de entregable5_ddl_completo.sql
-- y entregable8_populate_dimensiones.sql
-- ============================================================

-- Registros rechazados (documentados):
-- RECHAZADO: id_venta=1004 precio nulo (ventas_raw.csv)
-- RECHAZADO: id_venta=1005 fecha inválida '2024-13-07' (ventas_raw.csv)
-- RECHAZADO: id_venta=1008 cantidad inválida '-2' (ventas_raw.csv)
-- DUPLICADO ignorado: id_venta=1015 (ventas_nuevas.csv)
-- RECHAZADO: id_venta=1026 precio nulo (ventas_nuevas.csv)

-- ── Insertar clientes nuevos en dim_cliente ────────────────
INSERT INTO dwh.dim_cliente (id_cliente, email_cliente, telefono_cliente, ciudad, fecha_vigencia_desde, fecha_vigencia_hasta, es_registro_actual)
VALUES (2001, 'juan.cliente@email.com', '3001234567', 'Bogotá', '2024-01-01', NULL, TRUE)
ON CONFLICT (id_cliente) WHERE es_registro_actual = TRUE DO NOTHING;
INSERT INTO dwh.dim_cliente (id_cliente, email_cliente, telefono_cliente, ciudad, fecha_vigencia_desde, fecha_vigencia_hasta, es_registro_actual)
VALUES (2002, 'maria.gomez@email.com', '3009876543', 'Bogotá', '2024-01-01', NULL, TRUE)
ON CONFLICT (id_cliente) WHERE es_registro_actual = TRUE DO NOTHING;
INSERT INTO dwh.dim_cliente (id_cliente, email_cliente, telefono_cliente, ciudad, fecha_vigencia_desde, fecha_vigencia_hasta, es_registro_actual)
VALUES (2003, 'carlos.ruiz@email.com', '3102345678', 'Medellín', '2024-01-01', NULL, TRUE)
ON CONFLICT (id_cliente) WHERE es_registro_actual = TRUE DO NOTHING;
INSERT INTO dwh.dim_cliente (id_cliente, email_cliente, telefono_cliente, ciudad, fecha_vigencia_desde, fecha_vigencia_hasta, es_registro_actual)
VALUES (2006, 'sofia.lopez@email.com', '3201234567', 'Cali', '2024-01-01', NULL, TRUE)
ON CONFLICT (id_cliente) WHERE es_registro_actual = TRUE DO NOTHING;
INSERT INTO dwh.dim_cliente (id_cliente, email_cliente, telefono_cliente, ciudad, fecha_vigencia_desde, fecha_vigencia_hasta, es_registro_actual)
VALUES (2008, 'diana.herrera@email.com', '3176543210', 'Barranquilla', '2024-01-01', NULL, TRUE)
ON CONFLICT (id_cliente) WHERE es_registro_actual = TRUE DO NOTHING;
INSERT INTO dwh.dim_cliente (id_cliente, email_cliente, telefono_cliente, ciudad, fecha_vigencia_desde, fecha_vigencia_hasta, es_registro_actual)
VALUES (2009, 'miguel.torres@email.com', '3145678901', 'Bogotá', '2024-01-01', NULL, TRUE)
ON CONFLICT (id_cliente) WHERE es_registro_actual = TRUE DO NOTHING;
INSERT INTO dwh.dim_cliente (id_cliente, email_cliente, telefono_cliente, ciudad, fecha_vigencia_desde, fecha_vigencia_hasta, es_registro_actual)
VALUES (2010, 'isabella.mora@email.com', '3209876543', 'Cali', '2024-01-01', NULL, TRUE)
ON CONFLICT (id_cliente) WHERE es_registro_actual = TRUE DO NOTHING;
INSERT INTO dwh.dim_cliente (id_cliente, email_cliente, telefono_cliente, ciudad, fecha_vigencia_desde, fecha_vigencia_hasta, es_registro_actual)
VALUES (2011, 'daniel.rivera@email.com', '3134567890', 'Bogotá', '2024-01-01', NULL, TRUE)
ON CONFLICT (id_cliente) WHERE es_registro_actual = TRUE DO NOTHING;
INSERT INTO dwh.dim_cliente (id_cliente, email_cliente, telefono_cliente, ciudad, fecha_vigencia_desde, fecha_vigencia_hasta, es_registro_actual)
VALUES (2004, 'laura.santos@email.com', '3156789012', 'Cali', '2024-01-01', NULL, TRUE)
ON CONFLICT (id_cliente) WHERE es_registro_actual = TRUE DO NOTHING;
INSERT INTO dwh.dim_cliente (id_cliente, email_cliente, telefono_cliente, ciudad, fecha_vigencia_desde, fecha_vigencia_hasta, es_registro_actual)
VALUES (2012, 'valentina.garcia@email.com', '3198765432', 'Medellín', '2024-01-01', NULL, TRUE)
ON CONFLICT (id_cliente) WHERE es_registro_actual = TRUE DO NOTHING;
INSERT INTO dwh.dim_cliente (id_cliente, email_cliente, telefono_cliente, ciudad, fecha_vigencia_desde, fecha_vigencia_hasta, es_registro_actual)
VALUES (2013, 'santiago.mendez@email.com', '3112345678', 'Barranquilla', '2024-01-01', NULL, TRUE)
ON CONFLICT (id_cliente) WHERE es_registro_actual = TRUE DO NOTHING;
INSERT INTO dwh.dim_cliente (id_cliente, email_cliente, telefono_cliente, ciudad, fecha_vigencia_desde, fecha_vigencia_hasta, es_registro_actual)
VALUES (2014, 'camila.ortiz@email.com', '3187654321', 'Cali', '2024-01-01', NULL, TRUE)
ON CONFLICT (id_cliente) WHERE es_registro_actual = TRUE DO NOTHING;
INSERT INTO dwh.dim_cliente (id_cliente, email_cliente, telefono_cliente, ciudad, fecha_vigencia_desde, fecha_vigencia_hasta, es_registro_actual)
VALUES (2015, 'nicolas.ramirez@email.com', '3176543210', 'Medellín', '2024-01-01', NULL, TRUE)
ON CONFLICT (id_cliente) WHERE es_registro_actual = TRUE DO NOTHING;
INSERT INTO dwh.dim_cliente (id_cliente, email_cliente, telefono_cliente, ciudad, fecha_vigencia_desde, fecha_vigencia_hasta, es_registro_actual)
VALUES (2017, 'lucia.morales@email.com', '3145678902', 'Cali', '2024-01-01', NULL, TRUE)
ON CONFLICT (id_cliente) WHERE es_registro_actual = TRUE DO NOTHING;
INSERT INTO dwh.dim_cliente (id_cliente, email_cliente, telefono_cliente, ciudad, fecha_vigencia_desde, fecha_vigencia_hasta, es_registro_actual)
VALUES (2018, 'fernanda.silva@email.com', '3198765433', 'Medellín', '2024-01-01', NULL, TRUE)
ON CONFLICT (id_cliente) WHERE es_registro_actual = TRUE DO NOTHING;
INSERT INTO dwh.dim_cliente (id_cliente, email_cliente, telefono_cliente, ciudad, fecha_vigencia_desde, fecha_vigencia_hasta, es_registro_actual)
VALUES (2019, 'joaquin.castro@email.com', '3187654322', 'Cartagena', '2024-01-01', NULL, TRUE)
ON CONFLICT (id_cliente) WHERE es_registro_actual = TRUE DO NOTHING;
INSERT INTO dwh.dim_cliente (id_cliente, email_cliente, telefono_cliente, ciudad, fecha_vigencia_desde, fecha_vigencia_hasta, es_registro_actual)
VALUES (2020, 'gabriela.nunez@email.com', '3209876544', 'Bogotá', '2024-01-01', NULL, TRUE)
ON CONFLICT (id_cliente) WHERE es_registro_actual = TRUE DO NOTHING;
INSERT INTO dwh.dim_cliente (id_cliente, email_cliente, telefono_cliente, ciudad, fecha_vigencia_desde, fecha_vigencia_hasta, es_registro_actual)
VALUES (2021, 'ricardo.vargas@email.com', '3134567891', 'Medellín', '2024-01-01', NULL, TRUE)
ON CONFLICT (id_cliente) WHERE es_registro_actual = TRUE DO NOTHING;

-- ── Insertar en staging.ventas_clean ───────────────────────
INSERT INTO staging.ventas_clean (id_venta, fecha, id_cliente, id_producto, cantidad, precio_unitario, canal, ciudad, vendedor, email_cliente, telefono_cliente, id_batch)
VALUES (1001, '2024-01-05', 2001, 501, 2, 35000.0, 'Online', 'Bogotá', 'Juan Pérez', 'juan.cliente@email.com', '3001234567', 'batch_demo_20260311')
ON CONFLICT DO NOTHING;
INSERT INTO staging.ventas_clean (id_venta, fecha, id_cliente, id_producto, cantidad, precio_unitario, canal, ciudad, vendedor, email_cliente, telefono_cliente, id_batch)
VALUES (1002, '2024-01-05', 2002, 502, 1, 45000.0, 'Tienda', 'Bogotá', 'Ana Torres', 'maria.gomez@email.com', '3009876543', 'batch_demo_20260311')
ON CONFLICT DO NOTHING;
INSERT INTO staging.ventas_clean (id_venta, fecha, id_cliente, id_producto, cantidad, precio_unitario, canal, ciudad, vendedor, email_cliente, telefono_cliente, id_batch)
VALUES (1003, '2024-01-06', 2003, 503, 3, 15000.0, 'Online', 'Medellín', 'SIN_ASIGNAR', 'carlos.ruiz@email.com', '3102345678', 'batch_demo_20260311')
ON CONFLICT DO NOTHING;
INSERT INTO staging.ventas_clean (id_venta, fecha, id_cliente, id_producto, cantidad, precio_unitario, canal, ciudad, vendedor, email_cliente, telefono_cliente, id_batch)
VALUES (1006, '2024-01-08', 2001, 502, 5, 45000.0, 'Online', 'Bogotá', 'Juan Pérez', 'juan.cliente@email.com', '3001234567', 'batch_demo_20260311')
ON CONFLICT DO NOTHING;
INSERT INTO staging.ventas_clean (id_venta, fecha, id_cliente, id_producto, cantidad, precio_unitario, canal, ciudad, vendedor, email_cliente, telefono_cliente, id_batch)
VALUES (1007, '2024-01-08', 2006, 505, 1, 25000.0, 'Tienda', 'Cali', 'Laura Díaz', 'sofia.lopez@email.com', '3201234567', 'batch_demo_20260311')
ON CONFLICT DO NOTHING;
INSERT INTO staging.ventas_clean (id_venta, fecha, id_cliente, id_producto, cantidad, precio_unitario, canal, ciudad, vendedor, email_cliente, telefono_cliente, id_batch)
VALUES (1009, '2024-01-09', 2002, 506, 3, 55000.0, 'Marketplace', 'Bogotá', 'Ana Torres', 'maria.gomez@email.com', '3009876543', 'batch_demo_20260311')
ON CONFLICT DO NOTHING;
INSERT INTO staging.ventas_clean (id_venta, fecha, id_cliente, id_producto, cantidad, precio_unitario, canal, ciudad, vendedor, email_cliente, telefono_cliente, id_batch)
VALUES (1010, '2024-01-10', 2008, 503, 2, 15000.0, 'Tienda', 'Barranquilla', 'Roberto Vega', 'diana.herrera@email.com', '3176543210', 'batch_demo_20260311')
ON CONFLICT DO NOTHING;
INSERT INTO staging.ventas_clean (id_venta, fecha, id_cliente, id_producto, cantidad, precio_unitario, canal, ciudad, vendedor, email_cliente, telefono_cliente, id_batch)
VALUES (1011, '2024-01-10', 2009, 507, 1, 80000.0, 'Online', 'Bogotá', 'Juan Pérez', 'miguel.torres@email.com', '3145678901', 'batch_demo_20260311')
ON CONFLICT DO NOTHING;
INSERT INTO staging.ventas_clean (id_venta, fecha, id_cliente, id_producto, cantidad, precio_unitario, canal, ciudad, vendedor, email_cliente, telefono_cliente, id_batch)
VALUES (1012, '2024-01-11', 2003, 502, 4, 45000.0, 'Online', 'Medellín', 'SIN_ASIGNAR', 'carlos.ruiz@email.com', '3102345678', 'batch_demo_20260311')
ON CONFLICT DO NOTHING;
INSERT INTO staging.ventas_clean (id_venta, fecha, id_cliente, id_producto, cantidad, precio_unitario, canal, ciudad, vendedor, email_cliente, telefono_cliente, id_batch)
VALUES (1013, '2024-01-11', 2010, 508, 2, 30000.0, 'Tienda', 'Cali', 'Laura Díaz', 'isabella.mora@email.com', '3209876543', 'batch_demo_20260311')
ON CONFLICT DO NOTHING;
INSERT INTO staging.ventas_clean (id_venta, fecha, id_cliente, id_producto, cantidad, precio_unitario, canal, ciudad, vendedor, email_cliente, telefono_cliente, id_batch)
VALUES (1014, '2024-01-12', 2011, 501, 1, 35000.0, 'Marketplace', 'Bogotá', 'Juan Pérez', 'daniel.rivera@email.com', '3134567890', 'batch_demo_20260311')
ON CONFLICT DO NOTHING;
INSERT INTO staging.ventas_clean (id_venta, fecha, id_cliente, id_producto, cantidad, precio_unitario, canal, ciudad, vendedor, email_cliente, telefono_cliente, id_batch)
VALUES (1015, '2024-01-12', 2004, 509, 3, 20000.0, 'Online', 'Cali', 'Laura Díaz', 'laura.santos@email.com', '3156789012', 'batch_demo_20260311')
ON CONFLICT DO NOTHING;
INSERT INTO staging.ventas_clean (id_venta, fecha, id_cliente, id_producto, cantidad, precio_unitario, canal, ciudad, vendedor, email_cliente, telefono_cliente, id_batch)
VALUES (1016, '2024-01-13', 2012, 510, 5, 12000.0, 'Tienda', 'Medellín', 'Camilo Rojas', 'valentina.garcia@email.com', '3198765432', 'batch_demo_20260311')
ON CONFLICT DO NOTHING;
INSERT INTO staging.ventas_clean (id_venta, fecha, id_cliente, id_producto, cantidad, precio_unitario, canal, ciudad, vendedor, email_cliente, telefono_cliente, id_batch)
VALUES (1017, '2024-01-13', 2001, 503, 2, 15000.0, 'Online', 'Bogotá', 'Juan Pérez', 'juan.cliente@email.com', '3001234567', 'batch_demo_20260311')
ON CONFLICT DO NOTHING;
INSERT INTO staging.ventas_clean (id_venta, fecha, id_cliente, id_producto, cantidad, precio_unitario, canal, ciudad, vendedor, email_cliente, telefono_cliente, id_batch)
VALUES (1018, '2024-01-14', 2013, 502, 1, 45000.0, 'Tienda', 'Barranquilla', 'Roberto Vega', 'santiago.mendez@email.com', '3112345678', 'batch_demo_20260311')
ON CONFLICT DO NOTHING;
INSERT INTO staging.ventas_clean (id_venta, fecha, id_cliente, id_producto, cantidad, precio_unitario, canal, ciudad, vendedor, email_cliente, telefono_cliente, id_batch)
VALUES (1019, '2024-01-14', 2014, 504, 3, 22000.0, 'Online', 'Cali', 'Laura Díaz', 'camila.ortiz@email.com', '3187654321', 'batch_demo_20260311')
ON CONFLICT DO NOTHING;
INSERT INTO staging.ventas_clean (id_venta, fecha, id_cliente, id_producto, cantidad, precio_unitario, canal, ciudad, vendedor, email_cliente, telefono_cliente, id_batch)
VALUES (1020, '2024-01-14', 2015, 501, 2, 35000.0, 'Marketplace', 'Medellín', 'Camilo Rojas', 'nicolas.ramirez@email.com', '3176543210', 'batch_demo_20260311')
ON CONFLICT DO NOTHING;
INSERT INTO staging.ventas_clean (id_venta, fecha, id_cliente, id_producto, cantidad, precio_unitario, canal, ciudad, vendedor, email_cliente, telefono_cliente, id_batch)
VALUES (1021, '2024-01-15', 2001, 501, 1, 35000.0, 'Online', 'Bogotá', 'Juan Pérez', 'juan.cliente@email.com', '3001234567', 'batch_demo_20260311')
ON CONFLICT DO NOTHING;
INSERT INTO staging.ventas_clean (id_venta, fecha, id_cliente, id_producto, cantidad, precio_unitario, canal, ciudad, vendedor, email_cliente, telefono_cliente, id_batch)
VALUES (1022, '2024-01-15', 2017, 503, 2, 15000.0, 'Online', 'Cali', 'SIN_ASIGNAR', 'lucia.morales@email.com', '3145678902', 'batch_demo_20260311')
ON CONFLICT DO NOTHING;
INSERT INTO staging.ventas_clean (id_venta, fecha, id_cliente, id_producto, cantidad, precio_unitario, canal, ciudad, vendedor, email_cliente, telefono_cliente, id_batch)
VALUES (1023, '2024-01-16', 2018, 510, 3, 30000.0, 'Tienda', 'Medellín', 'Laura Díaz', 'fernanda.silva@email.com', '3198765433', 'batch_demo_20260311')
ON CONFLICT DO NOTHING;
INSERT INTO staging.ventas_clean (id_venta, fecha, id_cliente, id_producto, cantidad, precio_unitario, canal, ciudad, vendedor, email_cliente, telefono_cliente, id_batch)
VALUES (1024, '2024-01-16', 2002, 511, 1, 95000.0, 'Online', 'Bogotá', 'Ana Torres', 'maria.gomez@email.com', '3009876543', 'batch_demo_20260311')
ON CONFLICT DO NOTHING;
INSERT INTO staging.ventas_clean (id_venta, fecha, id_cliente, id_producto, cantidad, precio_unitario, canal, ciudad, vendedor, email_cliente, telefono_cliente, id_batch)
VALUES (1025, '2024-01-17', 2019, 502, 2, 45000.0, 'Marketplace', 'Cartagena', 'Roberto Vega', 'joaquin.castro@email.com', '3187654322', 'batch_demo_20260311')
ON CONFLICT DO NOTHING;
INSERT INTO staging.ventas_clean (id_venta, fecha, id_cliente, id_producto, cantidad, precio_unitario, canal, ciudad, vendedor, email_cliente, telefono_cliente, id_batch)
VALUES (1027, '2024-01-18', 2020, 501, 4, 35000.0, 'Online', 'Bogotá', 'Juan Pérez', 'gabriela.nunez@email.com', '3209876544', 'batch_demo_20260311')
ON CONFLICT DO NOTHING;
INSERT INTO staging.ventas_clean (id_venta, fecha, id_cliente, id_producto, cantidad, precio_unitario, canal, ciudad, vendedor, email_cliente, telefono_cliente, id_batch)
VALUES (1028, '2024-01-18', 2021, 503, 2, 15000.0, 'Online', 'Medellín', 'SIN_ASIGNAR', 'ricardo.vargas@email.com', '3134567891', 'batch_demo_20260311')
ON CONFLICT DO NOTHING;

-- ── Cargar fact_ventas desde staging.ventas_clean ──────────

INSERT INTO dwh.fact_ventas (
    id_venta, fecha_key, cliente_key, producto_key,
    vendedor_key, canal_key, ciudad_key,
    cantidad, precio_unitario, monto_total,
    descuento, impuestos, fecha_venta, id_batch
)
SELECT
    sc.id_venta,
    TO_CHAR(sc.fecha, 'YYYYMMDD')::INTEGER          AS fecha_key,
    dc.cliente_key,
    dp.producto_key,
    dv.vendedor_key,
    dca.canal_key,
    dci.ciudad_key,
    sc.cantidad,
    sc.precio_unitario,
    sc.monto_total,
    0, 0,
    sc.fecha,
    sc.id_batch
FROM staging.ventas_clean sc
JOIN dwh.dim_cliente   dc  ON dc.id_cliente      = sc.id_cliente  AND dc.es_registro_actual = TRUE
JOIN dwh.dim_producto  dp  ON dp.id_producto     = sc.id_producto
JOIN dwh.dim_vendedor  dv  ON dv.nombre_vendedor = sc.vendedor
JOIN dwh.dim_canal     dca ON dca.nombre_canal   = sc.canal
JOIN dwh.dim_ciudad    dci ON dci.nombre_ciudad  = sc.ciudad
JOIN dwh.dim_fecha     df  ON df.fecha           = sc.fecha
WHERE sc.id_batch = 'batch_demo_20260311'
ON CONFLICT (id_venta) DO NOTHING;


-- ── Verificación final ─────────────────────────────────────
SELECT COUNT(*) AS total_ventas, SUM(monto_total) AS monto_total FROM dwh.fact_ventas;
-- Resultado esperado: 24 ventas