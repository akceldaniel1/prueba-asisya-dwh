-- ============================================================
-- ENTREGABLE 18 – RBAC Y RLS EN POSTGRESQL
-- Fase 6 – Seguridad y Compliance
-- PostgreSQL 14+
-- ============================================================
-- Contenido:
--   SECCIÓN 1: Creación de roles (RBAC)
--   SECCIÓN 2: Asignación de permisos por schema/tabla
--   SECCIÓN 3: Column-Level Security (vistas restringidas)
--   SECCIÓN 4: Row-Level Security (CREATE POLICY)
--   SECCIÓN 5: Verificación de permisos
-- ============================================================


-- ============================================================
-- SECCIÓN 1: CREACIÓN DE ROLES (RBAC)
-- ============================================================
-- Tres roles con responsabilidades diferenciadas:
--   etl_user    → escribe datos en el pipeline ETL
--   report_user → solo lee vistas analíticas (sin datos personales)
--   admin       → acceso total para administración
-- ============================================================

-- Eliminar roles si ya existen (para re-ejecución limpia)
DROP ROLE IF EXISTS etl_user;
DROP ROLE IF EXISTS report_user;
DROP ROLE IF EXISTS admin_dwh;

-- ROL ETL: ejecuta el pipeline, escribe en staging y DWH
CREATE ROLE etl_user
    LOGIN
    PASSWORD 'etl_secure_pwd_2026!'
    CONNECTION LIMIT 5
    VALID UNTIL '2027-01-01';

COMMENT ON ROLE etl_user IS
    'Rol para el pipeline ETL – escribe en staging y DWH, sin acceso a datos personales sensibles';

-- ROL REPORTE: solo lectura de vistas analíticas
CREATE ROLE report_user
    LOGIN
    PASSWORD 'report_secure_pwd_2026!'
    CONNECTION LIMIT 20
    VALID UNTIL '2027-01-01';

COMMENT ON ROLE report_user IS
    'Rol para analistas y Power BI – lectura de vistas analíticas sin datos personales';

-- ROL ADMIN: administración completa del DWH
CREATE ROLE admin_dwh
    LOGIN
    PASSWORD 'admin_secure_pwd_2026!'
    SUPERUSER
    CONNECTION LIMIT 3
    VALID UNTIL '2027-01-01';

COMMENT ON ROLE admin_dwh IS
    'Rol administrador – acceso total para mantenimiento y auditoría';


-- ============================================================
-- SECCIÓN 2: PERMISOS POR SCHEMA Y TABLA
-- ============================================================

-- ── SCHEMA STAGING ──────────────────────────────────────────

-- etl_user: lectura y escritura en staging
GRANT USAGE  ON SCHEMA staging TO etl_user;
GRANT SELECT, INSERT, UPDATE, DELETE
    ON ALL TABLES IN SCHEMA staging TO etl_user;
GRANT USAGE, SELECT
    ON ALL SEQUENCES IN SCHEMA staging TO etl_user;

-- report_user: SIN acceso a staging (contiene datos personales sin limpiar)
REVOKE ALL ON SCHEMA staging FROM report_user;

-- ── SCHEMA DWH ──────────────────────────────────────────────

-- etl_user: escritura en todas las tablas del DWH
GRANT USAGE  ON SCHEMA dwh TO etl_user;
GRANT SELECT, INSERT, UPDATE
    ON ALL TABLES IN SCHEMA dwh TO etl_user;
GRANT USAGE, SELECT
    ON ALL SEQUENCES IN SCHEMA dwh TO etl_user;

-- report_user: solo lectura, y SOLO en vistas (no tablas base)
GRANT USAGE ON SCHEMA dwh TO report_user;
-- Revocar acceso a tablas base
REVOKE ALL ON dwh.fact_ventas    FROM report_user;
REVOKE ALL ON dwh.dim_cliente    FROM report_user;
-- Otorgar acceso solo a la vista analítica (sin datos personales)
GRANT SELECT ON dwh.v_ventas_completas  TO report_user;
GRANT SELECT ON dwh.dim_fecha           TO report_user;
GRANT SELECT ON dwh.dim_producto        TO report_user;
GRANT SELECT ON dwh.dim_vendedor        TO report_user;
GRANT SELECT ON dwh.dim_canal           TO report_user;
GRANT SELECT ON dwh.dim_ciudad          TO report_user;

-- ── SCHEMA CONTROL ──────────────────────────────────────────

-- etl_user: escritura en tablas de control (logs de calidad y ejecución)
GRANT USAGE  ON SCHEMA control TO etl_user;
GRANT SELECT, INSERT, UPDATE
    ON ALL TABLES IN SCHEMA control TO etl_user;
GRANT USAGE, SELECT
    ON ALL SEQUENCES IN SCHEMA control TO etl_user;

-- report_user: lectura de logs de calidad (sin auditoría de accesos)
GRANT USAGE  ON SCHEMA control TO report_user;
GRANT SELECT ON control.data_quality_log    TO report_user;
GRANT SELECT ON control.etl_execution_log   TO report_user;
-- SIN acceso a tabla de auditoría de accesos personales
REVOKE ALL ON control.auditoria_accesos FROM report_user;

-- Permisos por defecto para objetos futuros
ALTER DEFAULT PRIVILEGES IN SCHEMA dwh
    GRANT SELECT ON TABLES TO report_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO etl_user;


-- ============================================================
-- SECCIÓN 3: COLUMN-LEVEL SECURITY
-- ============================================================
-- report_user NO puede ver email_cliente ni telefono_cliente.
-- Se crea una vista restringida que enmascara esas columnas.
-- ============================================================

-- Vista de dim_cliente SIN datos personales para report_user
CREATE OR REPLACE VIEW dwh.v_cliente_anonimo AS
SELECT
    cliente_key,
    id_cliente,
    -- Enmascarar email: solo muestra dominio
    REGEXP_REPLACE(email_cliente, '^[^@]+', '***') AS email_cliente,
    -- Enmascarar teléfono: solo muestra últimos 4 dígitos
    CONCAT('******', RIGHT(telefono_cliente, 4))    AS telefono_cliente,
    ciudad,
    fecha_vigencia_desde,
    fecha_vigencia_hasta,
    es_registro_actual
FROM dwh.dim_cliente;

COMMENT ON VIEW dwh.v_cliente_anonimo IS
    'Vista de dim_cliente con email y teléfono enmascarados – accesible por report_user';

-- Otorgar acceso a la vista anonimizada (no a la tabla base)
GRANT SELECT ON dwh.v_cliente_anonimo TO report_user;
REVOKE ALL   ON dwh.dim_cliente       FROM report_user;

-- Vista de staging_clean SIN datos personales (para auditoría limitada)
CREATE OR REPLACE VIEW staging.v_ventas_clean_anonimo AS
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
    -- Enmascarar datos personales
    REGEXP_REPLACE(email_cliente, '^[^@]+', '***')  AS email_cliente,
    CONCAT('******', RIGHT(telefono_cliente, 4))     AS telefono_cliente,
    monto_total,
    id_batch
FROM staging.ventas_clean;

GRANT SELECT ON staging.v_ventas_clean_anonimo TO report_user;


-- ============================================================
-- SECCIÓN 4: ROW-LEVEL SECURITY (RLS)
-- ============================================================
-- RLS en PostgreSQL filtra filas a nivel de BD según el usuario.
-- Complementa el RLS de Power BI con una capa adicional de seguridad.
-- ============================================================

-- ── 4.1 Habilitar RLS en fact_ventas ────────────────────────
ALTER TABLE dwh.fact_ventas ENABLE ROW LEVEL SECURITY;
-- FORCE aplica RLS incluso al dueño de la tabla
ALTER TABLE dwh.fact_ventas FORCE ROW LEVEL SECURITY;

-- Política para etl_user: acceso total (necesita leer y escribir todo)
CREATE POLICY etl_full_access ON dwh.fact_ventas
    FOR ALL
    TO etl_user
    USING (TRUE);

-- Política para report_user: solo ve ventas de su región
-- (se une con dim_vendedor para verificar la región del usuario)
CREATE POLICY report_regional_access ON dwh.fact_ventas
    FOR SELECT
    TO report_user
    USING (
        vendedor_key IN (
            SELECT vendedor_key
            FROM dwh.dim_vendedor
            WHERE region = (
                -- Busca la región del usuario logueado en la tabla de usuarios
                SELECT region
                FROM dwh.dim_vendedor
                WHERE email_vendedor = current_user
                LIMIT 1
            )
        )
    );

COMMENT ON POLICY report_regional_access ON dwh.fact_ventas IS
    'report_user solo ve ventas de vendedores de su misma región';

-- ── 4.2 Habilitar RLS en dim_cliente ────────────────────────
ALTER TABLE dwh.dim_cliente ENABLE ROW LEVEL SECURITY;
ALTER TABLE dwh.dim_cliente FORCE ROW LEVEL SECURITY;

-- etl_user: acceso total
CREATE POLICY etl_cliente_access ON dwh.dim_cliente
    FOR ALL TO etl_user USING (TRUE);

-- report_user: no puede acceder (ya fue revocado arriba)
-- Solo accede via v_cliente_anonimo

-- ── 4.3 Habilitar RLS en auditoria_accesos ──────────────────
ALTER TABLE control.auditoria_accesos ENABLE ROW LEVEL SECURITY;
ALTER TABLE control.auditoria_accesos FORCE ROW LEVEL SECURITY;

-- Solo admin_dwh puede ver la tabla de auditoría
CREATE POLICY admin_auditoria_access ON control.auditoria_accesos
    FOR ALL TO admin_dwh USING (TRUE);

-- etl_user puede insertar (registrar accesos) pero no leer
CREATE POLICY etl_auditoria_insert ON control.auditoria_accesos
    FOR INSERT TO etl_user WITH CHECK (TRUE);


-- ============================================================
-- SECCIÓN 5: TRIGGER DE AUDITORÍA
-- ============================================================
-- Registra automáticamente cada SELECT sobre dim_cliente
-- (tabla con datos personales) en control.auditoria_accesos
-- ============================================================

-- Función del trigger
CREATE OR REPLACE FUNCTION control.fn_auditoria_acceso()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
    INSERT INTO control.auditoria_accesos (
        usuario_db,
        tabla_accedida,
        operacion,
        fecha_hora,
        ip_cliente
    )
    VALUES (
        current_user,
        TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME,
        TG_OP,
        CURRENT_TIMESTAMP,
        inet_client_addr()
    );
    RETURN NEW;
END;
$$;

COMMENT ON FUNCTION control.fn_auditoria_acceso() IS
    'Registra accesos a tablas con datos personales en control.auditoria_accesos';

-- Trigger en dim_cliente (datos personales)
DROP TRIGGER IF EXISTS trg_auditoria_cliente ON dwh.dim_cliente;
CREATE TRIGGER trg_auditoria_cliente
    AFTER INSERT OR UPDATE OR DELETE
    ON dwh.dim_cliente
    FOR EACH ROW
    EXECUTE FUNCTION control.fn_auditoria_acceso();

-- Trigger en staging.ventas_raw (contiene email y teléfono sin limpiar)
DROP TRIGGER IF EXISTS trg_auditoria_ventas_raw ON staging.ventas_raw;
CREATE TRIGGER trg_auditoria_ventas_raw
    AFTER INSERT OR UPDATE OR DELETE
    ON staging.ventas_raw
    FOR EACH ROW
    EXECUTE FUNCTION control.fn_auditoria_acceso();


-- ============================================================
-- SECCIÓN 6: VERIFICACIÓN DE PERMISOS
-- ============================================================

-- Ver todos los permisos de cada rol
SELECT
    grantee,
    table_schema,
    table_name,
    privilege_type
FROM information_schema.role_table_grants
WHERE grantee IN ('etl_user', 'report_user', 'admin_dwh')
ORDER BY grantee, table_schema, table_name;

-- Ver políticas RLS activas
SELECT
    schemaname,
    tablename,
    policyname,
    roles,
    cmd,
    qual
FROM pg_policies
ORDER BY schemaname, tablename;

-- Simular acceso como report_user
SET ROLE report_user;
-- Este query debe funcionar (vista sin datos personales):
SELECT * FROM dwh.v_ventas_completas LIMIT 3;
-- Este query debe fallar (tabla con datos personales):
-- SELECT email_cliente FROM dwh.dim_cliente;  -- ERROR esperado
RESET ROLE;
