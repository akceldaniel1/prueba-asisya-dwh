-- ============================================================
-- ENTREGABLE 20 – ANONIMIZACIÓN Y ENMASCARAMIENTO DE DATOS
-- Fase 6 – Seguridad y Compliance
-- PostgreSQL 14+
-- ============================================================
-- Contenido:
--   SECCIÓN 1: Funciones de enmascaramiento por tipo de dato
--   SECCIÓN 2: Función de anonimización completa de un registro
--   SECCIÓN 3: Vista de staging anonimizada para entornos no-prod
--   SECCIÓN 4: Script para generar dataset de pruebas anonimizado
--   SECCIÓN 5: Procedimiento de derecho al olvido (Ley 1581)
--   SECCIÓN 6: Procedimiento de portabilidad de datos
-- ============================================================


-- ============================================================
-- SECCIÓN 1: FUNCIONES DE ENMASCARAMIENTO
-- ============================================================

-- ── Enmascarar email ─────────────────────────────────────────
-- Conserva el dominio, reemplaza el usuario con ***
-- Ejemplo: juan.perez@gmail.com → ***@gmail.com
CREATE OR REPLACE FUNCTION control.fn_mask_email(email TEXT)
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    IF email IS NULL OR email = '' THEN
        RETURN NULL;
    END IF;
    RETURN '***@' || SPLIT_PART(email, '@', 2);
END;
$$;

COMMENT ON FUNCTION control.fn_mask_email(TEXT) IS
    'Enmascara email conservando el dominio. Uso: entornos no-productivos';

-- ── Enmascarar teléfono ──────────────────────────────────────
-- Conserva los últimos 4 dígitos
-- Ejemplo: 3001234567 → ******4567
CREATE OR REPLACE FUNCTION control.fn_mask_phone(telefono TEXT)
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    IF telefono IS NULL OR LENGTH(telefono) < 4 THEN
        RETURN '**********';
    END IF;
    RETURN REPEAT('*', LENGTH(telefono) - 4) ||
           RIGHT(telefono, 4);
END;
$$;

COMMENT ON FUNCTION control.fn_mask_phone(TEXT) IS
    'Enmascara teléfono conservando los últimos 4 dígitos';

-- ── Generar email ficticio determinístico ────────────────────
-- Genera un email falso pero reproducible a partir del id_cliente
-- Ejemplo: id_cliente=2001 → cliente_2001@anonimo.test
CREATE OR REPLACE FUNCTION control.fn_fake_email(id_cliente INTEGER)
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    RETURN 'cliente_' || id_cliente::TEXT || '@anonimo.test';
END;
$$;

COMMENT ON FUNCTION control.fn_fake_email(INTEGER) IS
    'Genera email ficticio determinístico a partir del id_cliente';

-- ── Anonimizar nombre de persona ─────────────────────────────
-- Conserva solo la inicial del primer nombre + apellido genérico
-- Ejemplo: Juan Pérez → J. Anónimo
CREATE OR REPLACE FUNCTION control.fn_mask_name(nombre TEXT)
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    inicial TEXT;
BEGIN
    IF nombre IS NULL OR nombre = '' OR nombre = 'SIN_ASIGNAR' THEN
        RETURN nombre;
    END IF;
    inicial := LEFT(TRIM(nombre), 1);
    RETURN inicial || '. Anónimo';
END;
$$;

COMMENT ON FUNCTION control.fn_mask_name(TEXT) IS
    'Conserva inicial del nombre y reemplaza apellido por "Anónimo"';

-- ── Pseudoanonimizar con hash ─────────────────────────────────
-- Genera un hash SHA256 del valor para análisis sin exponer el dato
-- El hash es irreversible pero consistente (mismo input → mismo hash)
CREATE OR REPLACE FUNCTION control.fn_hash_value(valor TEXT)
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    IF valor IS NULL THEN RETURN NULL; END IF;
    -- encode(sha256()) requiere extensión pgcrypto
    -- Si no está disponible, usar md5() como alternativa
    RETURN encode(
        digest(valor || 'salt_asisya_2026', 'sha256'),
        'hex'
    );
END;
$$;

COMMENT ON FUNCTION control.fn_hash_value(TEXT) IS
    'Genera hash SHA256 del valor para pseudoanonimización. Requiere pgcrypto.';

-- Instalar extensión pgcrypto si no está disponible
CREATE EXTENSION IF NOT EXISTS pgcrypto;


-- ============================================================
-- SECCIÓN 2: FUNCIÓN DE ANONIMIZACIÓN COMPLETA DE REGISTRO
-- ============================================================
-- Aplica todas las funciones de enmascaramiento a un registro
-- de staging.ventas_raw para entornos no-productivos
-- ============================================================

CREATE OR REPLACE FUNCTION control.fn_anonimizar_venta(
    p_id_cliente     INTEGER,
    p_email          TEXT,
    p_telefono       TEXT,
    p_vendedor       TEXT,
    OUT o_id_cliente INTEGER,
    OUT o_email      TEXT,
    OUT o_telefono   TEXT,
    OUT o_vendedor   TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
    -- id_cliente: conservar para análisis (es un ID, no dato personal)
    o_id_cliente := p_id_cliente;
    -- Email: generar ficticio determinístico
    o_email      := control.fn_fake_email(p_id_cliente);
    -- Teléfono: enmascarar conservando últimos 4 dígitos
    o_telefono   := control.fn_mask_phone(p_telefono);
    -- Vendedor: conservar (dato laboral, no personal del cliente)
    o_vendedor   := p_vendedor;
END;
$$;

COMMENT ON FUNCTION control.fn_anonimizar_venta IS
    'Anonimiza todos los datos personales de una venta. Para uso en entornos no-prod.';


-- ============================================================
-- SECCIÓN 3: VISTA ANONIMIZADA PARA ENTORNOS NO-PROD
-- ============================================================
-- En desarrollo y QA, los desarrolladores usan esta vista
-- en lugar de las tablas reales con datos personales
-- ============================================================

CREATE OR REPLACE VIEW staging.v_ventas_raw_anonimo AS
SELECT
    id_venta,
    fecha,
    id_cliente,
    id_producto,
    cantidad,
    precio_unitario,
    canal,
    ciudad,
    vendedor,                                          -- dato laboral, no personal
    control.fn_fake_email(id_cliente::INTEGER)         AS email_cliente,
    control.fn_mask_phone(telefono_cliente)            AS telefono_cliente,
    fecha_carga,
    archivo_origen,
    id_batch
FROM staging.ventas_raw;

COMMENT ON VIEW staging.v_ventas_raw_anonimo IS
    'Vista de ventas_raw con datos personales anonimizados para entornos no-productivos (Dev/QA)';

-- Vista anonimizada de dim_cliente
CREATE OR REPLACE VIEW dwh.v_cliente_dev AS
SELECT
    cliente_key,
    id_cliente,
    control.fn_fake_email(id_cliente)                  AS email_cliente,
    control.fn_mask_phone(telefono_cliente)            AS telefono_cliente,
    ciudad,                                            -- ciudad no es dato personal sensible
    fecha_vigencia_desde,
    fecha_vigencia_hasta,
    es_registro_actual,
    fecha_creacion
FROM dwh.dim_cliente;

COMMENT ON VIEW dwh.v_cliente_dev IS
    'Vista de dim_cliente con datos personales anonimizados para desarrollo y testing';

-- Otorgar acceso a las vistas anonimizadas
GRANT SELECT ON staging.v_ventas_raw_anonimo TO etl_user;
GRANT SELECT ON dwh.v_cliente_dev            TO etl_user;
GRANT SELECT ON dwh.v_cliente_dev            TO report_user;


-- ============================================================
-- SECCIÓN 4: SCRIPT PARA GENERAR DATASET DE PRUEBAS
-- ============================================================
-- Crea una copia anonimizada de los datos para usar en Dev/QA
-- Se ejecuta UNA VEZ al configurar un nuevo entorno
-- ============================================================

-- Crear esquema de pruebas si no existe
CREATE SCHEMA IF NOT EXISTS test_data;

-- Tabla de ventas anonimizada para pruebas
CREATE TABLE IF NOT EXISTS test_data.ventas_anonimo AS
SELECT
    id_venta,
    fecha,
    id_cliente,
    id_producto,
    cantidad,
    precio_unitario,
    canal,
    ciudad,
    vendedor,
    control.fn_fake_email(id_cliente::INTEGER) AS email_cliente,
    control.fn_mask_phone(telefono_cliente)    AS telefono_cliente,
    monto_total,
    id_batch
FROM staging.ventas_clean
LIMIT 0;  -- Estructura sin datos (se puebla con INSERT abajo)

-- Insertar datos reales anonimizados
INSERT INTO test_data.ventas_anonimo
SELECT
    id_venta,
    fecha,
    id_cliente,
    id_producto,
    cantidad,
    precio_unitario,
    canal,
    ciudad,
    vendedor,
    control.fn_fake_email(id_cliente::INTEGER),
    control.fn_mask_phone(telefono_cliente),
    monto_total,
    id_batch
FROM staging.ventas_clean;

COMMENT ON TABLE test_data.ventas_anonimo IS
    'Dataset anonimizado para entornos Dev/QA – generado desde staging.ventas_clean';


-- ============================================================
-- SECCIÓN 5: DERECHO AL OLVIDO (LEY 1581 / GDPR - Art. 17)
-- ============================================================
-- Procedimiento para eliminar todos los datos personales
-- de un cliente específico cuando lo solicite
-- ============================================================

CREATE OR REPLACE PROCEDURE control.sp_derecho_al_olvido(
    p_id_cliente INTEGER,
    p_motivo     TEXT DEFAULT 'Solicitud del titular'
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_registros_afectados INTEGER := 0;
BEGIN
    -- Paso 1: Registrar la solicitud en auditoría
    INSERT INTO control.auditoria_accesos (
        usuario_db, tabla_accedida, operacion, fecha_hora, filas_afectadas
    )
    VALUES (
        current_user,
        'DERECHO_AL_OLVIDO:id_cliente=' || p_id_cliente,
        'DELETE',
        CURRENT_TIMESTAMP,
        0
    );

    -- Paso 2: Anonimizar datos personales en dim_cliente
    -- (no eliminar el registro para no romper integridad referencial)
    UPDATE dwh.dim_cliente
    SET
        email_cliente    = control.fn_fake_email(id_cliente),
        telefono_cliente = '**********',
        fecha_actualizacion = CURRENT_TIMESTAMP
    WHERE id_cliente = p_id_cliente;

    GET DIAGNOSTICS v_registros_afectados = ROW_COUNT;

    -- Paso 3: Anonimizar en staging.ventas_raw
    UPDATE staging.ventas_raw
    SET
        email_cliente    = control.fn_fake_email(id_cliente::INTEGER),
        telefono_cliente = '**********'
    WHERE id_cliente = p_id_cliente::TEXT;

    -- Paso 4: Anonimizar en staging.ventas_clean
    UPDATE staging.ventas_clean
    SET
        email_cliente    = control.fn_fake_email(id_cliente),
        telefono_cliente = '**********'
    WHERE id_cliente = p_id_cliente;

    -- Paso 5: Registrar finalización en auditoría
    INSERT INTO control.auditoria_accesos (
        usuario_db, tabla_accedida, operacion, fecha_hora, filas_afectadas
    )
    VALUES (
        current_user,
        'DERECHO_AL_OLVIDO_COMPLETADO:id_cliente=' || p_id_cliente,
        'DELETE',
        CURRENT_TIMESTAMP,
        v_registros_afectados
    );

    RAISE NOTICE 'Derecho al olvido aplicado para id_cliente=%. Motivo: %. Registros afectados: %',
        p_id_cliente, p_motivo, v_registros_afectados;

EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'Error en derecho al olvido para id_cliente=%: %',
        p_id_cliente, SQLERRM;
END;
$$;

COMMENT ON PROCEDURE control.sp_derecho_al_olvido IS
    'Aplica el derecho al olvido (Art. 8 Ley 1581 / Art. 17 GDPR) anonimizando todos los datos personales del cliente sin romper la integridad referencial del DWH';

-- Ejemplo de uso:
-- CALL control.sp_derecho_al_olvido(2001, 'Solicitud enviada por email el 2026-03-11');


-- ============================================================
-- SECCIÓN 6: PORTABILIDAD DE DATOS (LEY 1581 / GDPR - Art. 20)
-- ============================================================
-- Genera un reporte de todos los datos de un cliente
-- en formato tabla, para entregárselo al titular
-- ============================================================

CREATE OR REPLACE FUNCTION control.fn_portabilidad_datos(
    p_id_cliente INTEGER
)
RETURNS TABLE (
    campo       TEXT,
    valor       TEXT,
    tabla_origen TEXT,
    fecha_registro TIMESTAMP
)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Datos de identificación del cliente
    RETURN QUERY
    SELECT
        'id_cliente'::TEXT,
        id_cliente::TEXT,
        'dwh.dim_cliente'::TEXT,
        fecha_creacion
    FROM dwh.dim_cliente
    WHERE id_cliente = p_id_cliente;

    RETURN QUERY
    SELECT 'email_cliente', email_cliente, 'dwh.dim_cliente', fecha_creacion
    FROM dwh.dim_cliente WHERE id_cliente = p_id_cliente AND es_registro_actual = TRUE;

    RETURN QUERY
    SELECT 'telefono_cliente', telefono_cliente, 'dwh.dim_cliente', fecha_creacion
    FROM dwh.dim_cliente WHERE id_cliente = p_id_cliente AND es_registro_actual = TRUE;

    RETURN QUERY
    SELECT 'ciudad', ciudad, 'dwh.dim_cliente', fecha_creacion
    FROM dwh.dim_cliente WHERE id_cliente = p_id_cliente AND es_registro_actual = TRUE;

    -- Historial de compras (sin datos de otros clientes)
    RETURN QUERY
    SELECT
        'venta_id_' || fv.id_venta::TEXT,
        'fecha=' || df.fecha::TEXT ||
        ', monto=' || fv.monto_total::TEXT ||
        ', canal=' || dca.nombre_canal ||
        ', ciudad=' || dci.nombre_ciudad,
        'dwh.fact_ventas',
        fv.fecha_carga
    FROM dwh.fact_ventas     fv
    JOIN dwh.dim_cliente      dc  ON dc.cliente_key  = fv.cliente_key
    JOIN dwh.dim_fecha         df  ON df.fecha_key    = fv.fecha_key
    JOIN dwh.dim_canal         dca ON dca.canal_key   = fv.canal_key
    JOIN dwh.dim_ciudad        dci ON dci.ciudad_key  = fv.ciudad_key
    WHERE dc.id_cliente = p_id_cliente;

    -- Registrar que se ejerció el derecho de acceso
    INSERT INTO control.auditoria_accesos (
        usuario_db, tabla_accedida, operacion, fecha_hora
    )
    VALUES (
        current_user,
        'PORTABILIDAD:id_cliente=' || p_id_cliente,
        'SELECT',
        CURRENT_TIMESTAMP
    );
END;
$$;

COMMENT ON FUNCTION control.fn_portabilidad_datos IS
    'Genera reporte completo de datos de un cliente para ejercer derecho de portabilidad (Art. 20 GDPR / Art. 8 Ley 1581)';

-- Ejemplo de uso:
-- SELECT * FROM control.fn_portabilidad_datos(2001);
