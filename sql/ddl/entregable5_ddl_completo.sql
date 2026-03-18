-- ============================================================
-- ENTREGABLE 5 – DDL COMPLETO DATA WAREHOUSE ASISYA
-- Fase 2 – Diseño del Data Warehouse
-- PostgreSQL 14+
-- Modelo: Esquema Estrella
-- ============================================================

-- ============================================================
-- SECCIÓN 1: SCHEMAS
-- ============================================================

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dwh;
CREATE SCHEMA IF NOT EXISTS control;

COMMENT ON SCHEMA staging IS 'Área de staging: datos crudos y transformados antes del DWH';
COMMENT ON SCHEMA dwh     IS 'Data Warehouse – modelo dimensional estrella';
COMMENT ON SCHEMA control IS 'Tablas de control, auditoría y calidad de datos';

-- ============================================================
-- SECCIÓN 2: STAGING
-- ============================================================

CREATE TABLE IF NOT EXISTS staging.ventas_raw (
    id_venta          VARCHAR(50),
    fecha             VARCHAR(50),
    id_cliente        VARCHAR(50),
    id_producto       VARCHAR(50),
    cantidad          VARCHAR(50),
    precio_unitario   VARCHAR(50),
    canal             VARCHAR(100),
    ciudad            VARCHAR(100),
    vendedor          VARCHAR(200),
    email_cliente     VARCHAR(200),
    telefono_cliente  VARCHAR(50),
    fecha_carga       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    archivo_origen    VARCHAR(200)
);

COMMENT ON TABLE staging.ventas_raw IS 'Datos crudos sin transformar, cargados directamente desde CSV';

CREATE TABLE IF NOT EXISTS staging.ventas_clean (
    id_venta            INTEGER       NOT NULL,
    fecha               DATE          NOT NULL,
    id_cliente          INTEGER       NOT NULL,
    id_producto         INTEGER       NOT NULL,
    cantidad            INTEGER       NOT NULL CHECK (cantidad > 0),
    precio_unitario     NUMERIC(12,2) NOT NULL CHECK (precio_unitario > 0),
    canal               VARCHAR(50)   NOT NULL,
    ciudad              VARCHAR(100)  NOT NULL,
    vendedor            VARCHAR(200),
    email_cliente       VARCHAR(200),
    telefono_cliente    VARCHAR(50),
    monto_total         NUMERIC(15,2) GENERATED ALWAYS AS (cantidad * precio_unitario) STORED,
    fecha_procesamiento TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    id_batch            VARCHAR(100)  NOT NULL
);

COMMENT ON TABLE staging.ventas_clean IS 'Datos validados y transformados, listos para carga al DWH';

-- ============================================================
-- SECCIÓN 3: DIMENSIÓN FECHA
-- ============================================================
-- Decisión de diseño:
--   fecha_key = INTEGER en formato YYYYMMDD (ej: 20240105)
--   Permite joins directos sin conversiones de tipo.
--   Rango: 2020-01-01 a 2030-12-31 (4018 registros)
--   Incluye festivos colombianos fijos y móviles.
-- ============================================================

CREATE TABLE IF NOT EXISTS dwh.dim_fecha (
    fecha_key           INTEGER      PRIMARY KEY,         -- YYYYMMDD
    fecha               DATE         NOT NULL UNIQUE,
    anio                SMALLINT     NOT NULL,
    mes                 SMALLINT     NOT NULL CHECK (mes BETWEEN 1 AND 12),
    dia                 SMALLINT     NOT NULL CHECK (dia BETWEEN 1 AND 31),
    nombre_mes          VARCHAR(20)  NOT NULL,
    trimestre           SMALLINT     NOT NULL CHECK (trimestre BETWEEN 1 AND 4),
    semana_anio         SMALLINT     NOT NULL,
    dia_semana          SMALLINT     NOT NULL CHECK (dia_semana BETWEEN 1 AND 7), -- 1=Lunes ISO
    nombre_dia_semana   VARCHAR(20)  NOT NULL,
    es_fin_semana       BOOLEAN      NOT NULL DEFAULT FALSE,
    es_festivo          BOOLEAN      NOT NULL DEFAULT FALSE,
    nombre_festivo      VARCHAR(100),
    fecha_creacion      TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE  dwh.dim_fecha IS 'Dimensión tiempo con rango 2020-2030 y festivos colombianos';
COMMENT ON COLUMN dwh.dim_fecha.fecha_key        IS 'Surrogate key en formato YYYYMMDD';
COMMENT ON COLUMN dwh.dim_fecha.dia_semana        IS '1=Lunes, 7=Domingo (estándar ISO)';
COMMENT ON COLUMN dwh.dim_fecha.nombre_festivo    IS 'Nombre del festivo colombiano si aplica, NULL si no es festivo';

-- ============================================================
-- SECCIÓN 4: DIMENSIÓN CLIENTE (SCD Type 2)
-- ============================================================
-- Decisión de diseño:
--   SCD Type 2 para trackear cambios en: ciudad, email, teléfono.
--   cliente_key es el surrogate key (SERIAL).
--   id_cliente es el natural key del sistema fuente.
--   Un cliente puede tener múltiples registros históricos,
--   pero solo UNO con es_registro_actual = TRUE.
--   El índice único parcial garantiza esto a nivel de BD.
-- ============================================================

CREATE TABLE IF NOT EXISTS dwh.dim_cliente (
    cliente_key             SERIAL        PRIMARY KEY,
    id_cliente              INTEGER       NOT NULL,
    -- Atributos tracked con SCD Type 2
    email_cliente           VARCHAR(200),
    telefono_cliente        VARCHAR(50),
    ciudad                  VARCHAR(100),
    -- Control SCD Type 2
    fecha_vigencia_desde    DATE          NOT NULL,
    fecha_vigencia_hasta    DATE,                    -- NULL = vigente actualmente
    es_registro_actual      BOOLEAN       NOT NULL DEFAULT TRUE,
    -- Auditoría
    fecha_creacion          TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    fecha_actualizacion     TIMESTAMP
);

-- Solo puede existir un registro activo por cliente
CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_cliente_activo
    ON dwh.dim_cliente (id_cliente)
    WHERE es_registro_actual = TRUE;

COMMENT ON TABLE  dwh.dim_cliente IS 'Dimensión cliente con SCD Type 2 – trackea cambios históricos en ciudad, email y teléfono';
COMMENT ON COLUMN dwh.dim_cliente.cliente_key          IS 'Surrogate key generada por secuencia – no tiene significado de negocio';
COMMENT ON COLUMN dwh.dim_cliente.id_cliente           IS 'Natural key del sistema de origen';
COMMENT ON COLUMN dwh.dim_cliente.fecha_vigencia_hasta IS 'NULL indica registro vigente. Se actualiza cuando el cliente cambia atributos';
COMMENT ON COLUMN dwh.dim_cliente.es_registro_actual   IS 'TRUE solo en la versión más reciente del cliente';

-- ============================================================
-- SECCIÓN 5: DIMENSIÓN PRODUCTO
-- ============================================================
-- Decisión de diseño:
--   SCD Type 1 (sobreescritura) para cambios de precio de referencia
--   y categoría. Los precios históricos de venta están en fact_ventas.
--   Se incluye jerarquía: categoria > subcategoria para drill-down.
-- ============================================================

CREATE TABLE IF NOT EXISTS dwh.dim_producto (
    producto_key        SERIAL        PRIMARY KEY,
    id_producto         INTEGER       NOT NULL UNIQUE,
    nombre_producto     VARCHAR(200)  NOT NULL,
    categoria           VARCHAR(100)  NOT NULL,
    subcategoria        VARCHAR(100),
    precio_referencia   NUMERIC(12,2) CHECK (precio_referencia > 0),
    unidad_medida       VARCHAR(50)   DEFAULT 'Unidad',
    activo              BOOLEAN       NOT NULL DEFAULT TRUE,
    fecha_creacion      TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    fecha_actualizacion TIMESTAMP
);

COMMENT ON TABLE  dwh.dim_producto IS 'Dimensión producto con jerarquía categoría/subcategoría – SCD Type 1';
COMMENT ON COLUMN dwh.dim_producto.precio_referencia IS 'Precio de lista (referencia). El precio real de venta está en fact_ventas';
COMMENT ON COLUMN dwh.dim_producto.activo            IS 'FALSE si el producto fue descontinuado';

-- ============================================================
-- SECCIÓN 6: DIMENSIÓN VENDEDOR
-- ============================================================
-- Decisión de diseño:
--   SCD Type 1. Los cambios de zona/región se sobreescriben.
--   nombre_vendedor como natural key (único en la empresa).
--   Se incluye región para Row-Level Security en Power BI.
-- ============================================================

CREATE TABLE IF NOT EXISTS dwh.dim_vendedor (
    vendedor_key        SERIAL        PRIMARY KEY,
    nombre_vendedor     VARCHAR(200)  NOT NULL UNIQUE,
    ciudad_base         VARCHAR(100),
    region              VARCHAR(100),
    zona                VARCHAR(100),
    fecha_ingreso       DATE,
    estado              VARCHAR(20)   NOT NULL DEFAULT 'Activo'
                            CHECK (estado IN ('Activo','Inactivo','Vacaciones')),
    fecha_creacion      TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    fecha_actualizacion TIMESTAMP
);

COMMENT ON TABLE  dwh.dim_vendedor IS 'Dimensión vendedor – incluye región para RLS en reportes';
COMMENT ON COLUMN dwh.dim_vendedor.region IS 'Usada para Row-Level Security: Gerente Regional ve solo su región';

-- Registro especial para ventas sin vendedor asignado
INSERT INTO dwh.dim_vendedor (nombre_vendedor, estado)
VALUES ('SIN_ASIGNAR', 'Activo')
ON CONFLICT (nombre_vendedor) DO NOTHING;

-- ============================================================
-- SECCIÓN 7: DIMENSIÓN CANAL
-- ============================================================

CREATE TABLE IF NOT EXISTS dwh.dim_canal (
    canal_key       SERIAL        PRIMARY KEY,
    nombre_canal    VARCHAR(50)   NOT NULL UNIQUE,
    tipo_canal      VARCHAR(50),                   -- Digital, Físico, Mixto
    descripcion     VARCHAR(200),
    comision_pct    NUMERIC(5,2)  DEFAULT 0 CHECK (comision_pct >= 0),
    fecha_creacion  TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE dwh.dim_canal IS 'Dimensión canal de venta: Online, Tienda, Marketplace';

-- ============================================================
-- SECCIÓN 8: DIMENSIÓN CIUDAD
-- ============================================================
-- Decisión de diseño:
--   Se incluye departamento y región para análisis geográfico
--   jerárquico: ciudad > departamento > región.
--   Latitud/longitud opcionales para mapas en Power BI.
-- ============================================================

CREATE TABLE IF NOT EXISTS dwh.dim_ciudad (
    ciudad_key      SERIAL        PRIMARY KEY,
    nombre_ciudad   VARCHAR(100)  NOT NULL UNIQUE,
    departamento    VARCHAR(100),
    region          VARCHAR(100),
    poblacion       INTEGER       CHECK (poblacion > 0),
    latitud         NUMERIC(9,6),
    longitud        NUMERIC(9,6),
    fecha_creacion  TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE dwh.dim_ciudad IS 'Dimensión geográfica con jerarquía ciudad > departamento > región';

-- ============================================================
-- SECCIÓN 9: TABLA DE HECHOS – fact_ventas
-- ============================================================
-- Decisión de diseño:
--   UNIQUE en id_venta garantiza idempotencia (re-ejecución segura).
--   monto_neto es columna generada (no requiere cálculo en app).
--   descuento e impuestos en 0 por defecto (se populan si aplica).
--   fecha_venta desnormalizada para queries sin join a dim_fecha.
-- ============================================================

CREATE TABLE IF NOT EXISTS dwh.fact_ventas (
    venta_key       BIGSERIAL     PRIMARY KEY,
    id_venta        INTEGER       NOT NULL,
    -- Foreign Keys
    fecha_key       INTEGER       NOT NULL,
    cliente_key     INTEGER       NOT NULL,
    producto_key    INTEGER       NOT NULL,
    vendedor_key    INTEGER       NOT NULL,
    canal_key       INTEGER       NOT NULL,
    ciudad_key      INTEGER       NOT NULL,
    -- Medidas
    cantidad        INTEGER       NOT NULL CHECK (cantidad > 0),
    precio_unitario NUMERIC(12,2) NOT NULL CHECK (precio_unitario > 0),
    monto_total     NUMERIC(15,2) NOT NULL CHECK (monto_total > 0),
    descuento       NUMERIC(10,2) NOT NULL DEFAULT 0 CHECK (descuento >= 0),
    impuestos       NUMERIC(10,2) NOT NULL DEFAULT 0 CHECK (impuestos >= 0),
    monto_neto      NUMERIC(15,2) GENERATED ALWAYS AS
                        (monto_total - descuento + impuestos) STORED,
    -- Auditoría
    fecha_venta     DATE          NOT NULL,   -- desnormalizado para queries rápidos
    fecha_carga     TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    id_batch        VARCHAR(100)  NOT NULL,
    -- Constraints
    CONSTRAINT uq_fact_id_venta  UNIQUE (id_venta),
    CONSTRAINT fk_fact_fecha     FOREIGN KEY (fecha_key)    REFERENCES dwh.dim_fecha(fecha_key),
    CONSTRAINT fk_fact_cliente   FOREIGN KEY (cliente_key)  REFERENCES dwh.dim_cliente(cliente_key),
    CONSTRAINT fk_fact_producto  FOREIGN KEY (producto_key) REFERENCES dwh.dim_producto(producto_key),
    CONSTRAINT fk_fact_vendedor  FOREIGN KEY (vendedor_key) REFERENCES dwh.dim_vendedor(vendedor_key),
    CONSTRAINT fk_fact_canal     FOREIGN KEY (canal_key)    REFERENCES dwh.dim_canal(canal_key),
    CONSTRAINT fk_fact_ciudad    FOREIGN KEY (ciudad_key)   REFERENCES dwh.dim_ciudad(ciudad_key)
);

COMMENT ON TABLE  dwh.fact_ventas IS 'Tabla de hechos central – transacciones de ventas';
COMMENT ON COLUMN dwh.fact_ventas.id_venta    IS 'Natural key del sistema fuente – UNIQUE garantiza idempotencia';
COMMENT ON COLUMN dwh.fact_ventas.monto_neto  IS 'Columna generada: monto_total - descuento + impuestos';
COMMENT ON COLUMN dwh.fact_ventas.fecha_venta IS 'Desnormalizado desde dim_fecha para agilizar queries de rango de fechas';

-- ============================================================
-- SECCIÓN 10: TABLAS DE CONTROL
-- ============================================================

CREATE TABLE IF NOT EXISTS control.etl_execution_log (
    log_id               SERIAL       PRIMARY KEY,
    proceso              VARCHAR(100) NOT NULL,
    fecha_inicio         TIMESTAMP    NOT NULL,
    fecha_fin            TIMESTAMP,
    estado               VARCHAR(20)  CHECK (estado IN ('INICIADO','EXITOSO','FALLIDO','PARCIAL')),
    registros_procesados INTEGER,
    registros_exitosos   INTEGER,
    registros_fallidos   INTEGER,
    mensaje_error        TEXT,
    id_batch             VARCHAR(100),
    usuario              VARCHAR(100),
    servidor             VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS control.data_quality_log (
    calidad_id           SERIAL        PRIMARY KEY,
    tabla_origen         VARCHAR(100),
    regla_calidad        VARCHAR(200),
    fecha_validacion     TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    registros_evaluados  INTEGER,
    registros_validos    INTEGER,
    registros_invalidos  INTEGER,
    porcentaje_calidad   NUMERIC(5,2),
    detalles             TEXT,
    id_batch             VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS control.auditoria_accesos (
    auditoria_id    SERIAL       PRIMARY KEY,
    usuario_db      VARCHAR(100) NOT NULL DEFAULT CURRENT_USER,
    tabla_accedida  VARCHAR(200) NOT NULL,
    operacion       VARCHAR(10)  NOT NULL CHECK (operacion IN ('SELECT','INSERT','UPDATE','DELETE')),
    fecha_hora      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ip_cliente      INET,
    filas_afectadas INTEGER
);

COMMENT ON TABLE control.auditoria_accesos IS 'Registro de accesos a datos personales – compliance Ley 1581 / GDPR';

-- ============================================================
-- SECCIÓN 11: ÍNDICES ESTRATÉGICOS
-- ============================================================

-- Staging
CREATE INDEX IF NOT EXISTS idx_stg_raw_fecha_carga  ON staging.ventas_raw   (fecha_carga);
CREATE INDEX IF NOT EXISTS idx_stg_clean_batch       ON staging.ventas_clean  (id_batch);
CREATE INDEX IF NOT EXISTS idx_stg_clean_id_venta    ON staging.ventas_clean  (id_venta);

-- dim_fecha
CREATE INDEX IF NOT EXISTS idx_dim_fecha_anio_mes    ON dwh.dim_fecha (anio, mes);
CREATE INDEX IF NOT EXISTS idx_dim_fecha_trimestre    ON dwh.dim_fecha (anio, trimestre);

-- dim_cliente
CREATE INDEX IF NOT EXISTS idx_dim_cli_id_cliente    ON dwh.dim_cliente (id_cliente);

-- dim_producto
CREATE INDEX IF NOT EXISTS idx_dim_prod_categoria    ON dwh.dim_producto (categoria, subcategoria);

-- fact_ventas – índices simples por FK
CREATE INDEX IF NOT EXISTS idx_fv_fecha_key          ON dwh.fact_ventas (fecha_key);
CREATE INDEX IF NOT EXISTS idx_fv_cliente_key        ON dwh.fact_ventas (cliente_key);
CREATE INDEX IF NOT EXISTS idx_fv_producto_key       ON dwh.fact_ventas (producto_key);
CREATE INDEX IF NOT EXISTS idx_fv_vendedor_key       ON dwh.fact_ventas (vendedor_key);
CREATE INDEX IF NOT EXISTS idx_fv_canal_key          ON dwh.fact_ventas (canal_key);
CREATE INDEX IF NOT EXISTS idx_fv_ciudad_key         ON dwh.fact_ventas (ciudad_key);
CREATE INDEX IF NOT EXISTS idx_fv_batch              ON dwh.fact_ventas (id_batch);
CREATE INDEX IF NOT EXISTS idx_fv_fecha_venta        ON dwh.fact_ventas (fecha_venta DESC);

-- fact_ventas – índices compuestos para queries analíticos frecuentes
CREATE INDEX IF NOT EXISTS idx_fv_fecha_producto     ON dwh.fact_ventas (fecha_key, producto_key);
CREATE INDEX IF NOT EXISTS idx_fv_vendedor_fecha     ON dwh.fact_ventas (vendedor_key, fecha_key);
CREATE INDEX IF NOT EXISTS idx_fv_ciudad_canal       ON dwh.fact_ventas (ciudad_key, canal_key);

-- ============================================================
-- SECCIÓN 12: VISTA ANALÍTICA
-- ============================================================

CREATE OR REPLACE VIEW dwh.v_ventas_completas AS
SELECT
    fv.id_venta,
    df.fecha,
    df.anio,
    df.nombre_mes,
    df.trimestre,
    df.es_festivo,
    dc.id_cliente,
    dc.ciudad                AS ciudad_cliente,
    dp.nombre_producto,
    dp.categoria,
    dp.subcategoria,
    dv.nombre_vendedor,
    dv.region                AS region_vendedor,
    dca.nombre_canal,
    dci.nombre_ciudad        AS ciudad_venta,
    dci.departamento,
    dci.region               AS region_ciudad,
    fv.cantidad,
    fv.precio_unitario,
    fv.monto_total,
    fv.descuento,
    fv.impuestos,
    fv.monto_neto,
    fv.id_batch
FROM      dwh.fact_ventas    fv
JOIN      dwh.dim_fecha      df  ON df.fecha_key    = fv.fecha_key
JOIN      dwh.dim_cliente    dc  ON dc.cliente_key  = fv.cliente_key
JOIN      dwh.dim_producto   dp  ON dp.producto_key = fv.producto_key
JOIN      dwh.dim_vendedor   dv  ON dv.vendedor_key = fv.vendedor_key
JOIN      dwh.dim_canal      dca ON dca.canal_key   = fv.canal_key
JOIN      dwh.dim_ciudad     dci ON dci.ciudad_key  = fv.ciudad_key;

COMMENT ON VIEW dwh.v_ventas_completas IS 'Vista denormalizada lista para Power BI y reportes analíticos';
