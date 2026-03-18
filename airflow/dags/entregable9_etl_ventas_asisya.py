"""
================================================================
ENTREGABLE 9 – DAG ETL VENTAS ASISYA
Fase 3 – Pipeline ETL con Apache Airflow
================================================================
Descripción:
    Pipeline ETL completo para procesamiento de ventas.
    Maneja carga inicial desde ventas_raw.csv y carga
    incremental desde ventas_nuevas.csv.

Flujo:
    extract_csv → profile_validate → clean_transform →
    load_dimensions → load_fact → cleanup_staging →
    notify_success

Características:
    - Idempotencia via batch_id y UNIQUE en id_venta
    - SCD Type 2 en dim_cliente
    - Manejo de errores con rollback
    - Logging en control.etl_execution_log
    - Logging de calidad en control.data_quality_log
    - Soporte carga inicial e incremental

Conexiones requeridas en Airflow UI:
    - postgres_dwh: Conexión PostgreSQL al DWH

Variables requeridas en Airflow UI:
    - csv_raw_path:       ruta a ventas_raw.csv
    - csv_new_path:       ruta a ventas_nuevas.csv
    - quality_threshold:  umbral mínimo de calidad (default 0.75)
    - retention_days:     días retención staging (default 7)
================================================================
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.utils.dates import days_ago

from datetime import datetime, timedelta, date
import pandas as pd
import numpy as np
import logging
import uuid
import re
import os

logger = logging.getLogger(__name__)

# ================================================================
# CONFIGURACIÓN POR DEFECTO
# ================================================================

default_args = {
    'owner':             'data-engineer',
    'depends_on_past':   False,
    'email_on_failure':  True,
    'email_on_retry':    False,
    'email':             ['data-team@asisya.com'],
    'retries':           2,
    'retry_delay':       timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

POSTGRES_CONN  = 'postgres_dwh'
QUALITY_THRESH = 0.75
RETENTION_DAYS = 7

# Canales válidos y su normalización
CANAL_MAP = {
    'online':      'Online',
    'tienda':      'Tienda',
    'marketplace': 'Marketplace',
}

# ================================================================
# UTILIDADES GENERALES
# ================================================================

def generate_batch_id() -> str:
    """Genera ID único para el batch de procesamiento."""
    return f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"


def get_pg_hook() -> PostgresHook:
    """Retorna hook de PostgreSQL configurado."""
    return PostgresHook(postgres_conn_id=POSTGRES_CONN)


def normalize_date(val) -> date | None:
    """Parsea fechas en formatos YYYY-MM-DD o YYYY/MM/DD."""
    if pd.isna(val):
        return None
    val = str(val).strip()
    for fmt in ('%Y-%m-%d', '%Y/%m/%d', '%d-%m-%Y', '%d/%m/%Y'):
        try:
            return datetime.strptime(val, fmt).date()
        except ValueError:
            continue
    return None


def normalize_canal(val) -> str | None:
    """Estandariza canal a Online / Tienda / Marketplace."""
    if pd.isna(val) or str(val).strip() == '':
        return None
    return CANAL_MAP.get(str(val).strip().lower(), str(val).strip())


def is_valid_email(val) -> bool:
    """Valida formato básico de email."""
    if pd.isna(val) or str(val).strip() == '':
        return False
    return bool(re.match(r'^[\w\.-]+@[\w\.-]+\.\w{2,}$', str(val).strip()))


# ================================================================
# LOGGING A TABLAS DE CONTROL
# ================================================================

def log_etl_start(hook: PostgresHook, proceso: str, batch_id: str) -> int:
    """Registra inicio de proceso ETL. Retorna log_id."""
    result = hook.get_first("""
        INSERT INTO control.etl_execution_log
            (proceso, fecha_inicio, estado, id_batch, usuario, servidor)
        VALUES (%s, NOW(), 'INICIADO', %s, CURRENT_USER, inet_server_addr()::TEXT)
        RETURNING log_id
    """, parameters=(proceso, batch_id))
    return result[0] if result else None


def log_etl_end(hook: PostgresHook, log_id: int, estado: str,
                procesados: int, exitosos: int, fallidos: int,
                error: str = None):
    """Actualiza registro de ejecución ETL con resultado final."""
    hook.run("""
        UPDATE control.etl_execution_log
        SET fecha_fin            = NOW(),
            estado               = %s,
            registros_procesados = %s,
            registros_exitosos   = %s,
            registros_fallidos   = %s,
            mensaje_error        = %s
        WHERE log_id = %s
    """, parameters=(estado, procesados, exitosos, fallidos, error, log_id))


def log_quality(hook: PostgresHook, tabla: str, regla: str,
                evaluados: int, validos: int, invalidos: int,
                detalles: str, batch_id: str):
    """Registra resultado de validación en control.data_quality_log."""
    pct = round(validos / evaluados * 100, 2) if evaluados > 0 else 0
    hook.run("""
        INSERT INTO control.data_quality_log
            (tabla_origen, regla_calidad, registros_evaluados,
             registros_validos, registros_invalidos,
             porcentaje_calidad, detalles, id_batch)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, parameters=(tabla, regla, evaluados, validos, invalidos, pct, detalles, batch_id))


# ================================================================
# TASK 1: EXTRACCIÓN
# ================================================================

def extract_from_csv(**context):
    """
    Lee ventas_raw.csv y lo carga a staging.ventas_raw.
    Genera batch_id y lo pasa via XCom a las siguientes tasks.
    Soporta carga inicial e incremental según parámetro 'mode'.
    """
    mode     = context['params'].get('mode', 'initial')
    batch_id = generate_batch_id()
    context['ti'].xcom_push(key='batch_id', value=batch_id)
    context['ti'].xcom_push(key='mode',     value=mode)

    # Determinar archivo según modo
    if mode == 'incremental':
        csv_path = Variable.get('csv_new_path',
                    default_var='/opt/airflow/data/ventas_nuevas.csv')
        archivo  = 'ventas_nuevas.csv'
    else:
        csv_path = Variable.get('csv_raw_path',
                    default_var='/opt/airflow/data/ventas_raw.csv')
        archivo  = 'ventas_raw.csv'

    logger.info(f"[EXTRACT] Iniciando – Batch: {batch_id} | Modo: {mode} | Archivo: {archivo}")

    hook   = get_pg_hook()
    log_id = log_etl_start(hook, 'extract_from_csv', batch_id)

    try:
        # Leer CSV con todo como string para análisis posterior
        df = pd.read_csv(csv_path, dtype=str)
        df = df.fillna('')
        logger.info(f"[EXTRACT] CSV leído: {len(df)} registros, {len(df.columns)} columnas")

        # Limpiar staging previo del mismo batch (idempotencia)
        hook.run(
            "DELETE FROM staging.ventas_raw WHERE id_batch = %s",
            parameters=(batch_id,)
        )

        # Insertar en staging.ventas_raw
        rows_inserted = 0
        for _, row in df.iterrows():
            hook.run("""
                INSERT INTO staging.ventas_raw (
                    id_venta, fecha, id_cliente, id_producto,
                    cantidad, precio_unitario, canal, ciudad,
                    vendedor, email_cliente, telefono_cliente,
                    archivo_origen, id_batch
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, parameters=(
                row.get('id_venta',''),       row.get('fecha',''),
                row.get('id_cliente',''),     row.get('id_producto',''),
                row.get('cantidad',''),        row.get('precio_unitario',''),
                row.get('canal',''),           row.get('ciudad',''),
                row.get('vendedor',''),        row.get('email_cliente',''),
                row.get('telefono_cliente',''),archivo,
                batch_id
            ))
            rows_inserted += 1

        context['ti'].xcom_push(key='rows_extracted', value=rows_inserted)
        log_etl_end(hook, log_id, 'EXITOSO', rows_inserted, rows_inserted, 0)
        logger.info(f"[EXTRACT] Completado – {rows_inserted} registros cargados a staging.ventas_raw")

    except Exception as e:
        log_etl_end(hook, log_id, 'FALLIDO', 0, 0, 0, str(e))
        logger.error(f"[EXTRACT] Error: {e}")
        raise


# ================================================================
# TASK 2: PROFILING Y VALIDACIÓN DE CALIDAD
# ================================================================

def profile_and_validate(**context):
    """
    Lee staging.ventas_raw, aplica todas las reglas de calidad
    definidas en la Fase 1, y registra resultados en
    control.data_quality_log.
    Aborta el pipeline si el score cae por debajo del umbral.
    """
    batch_id = context['ti'].xcom_pull(key='batch_id')
    hook     = get_pg_hook()
    log_id   = log_etl_start(hook, 'profile_and_validate', batch_id)

    logger.info(f"[VALIDATE] Iniciando – Batch: {batch_id}")

    try:
        # Leer datos del batch actual
        rows = hook.get_records(
            "SELECT * FROM staging.ventas_raw WHERE id_batch = %s",
            parameters=(batch_id,)
        )
        cols = ['id_venta','fecha','id_cliente','id_producto','cantidad',
                'precio_unitario','canal','ciudad','vendedor',
                'email_cliente','telefono_cliente','fecha_carga','archivo_origen','id_batch']

        # staging.ventas_raw no tiene id_batch como columna en el schema base
        # Lo obtenemos del query como está definido en el DDL extendido
        df = pd.DataFrame(rows, columns=[
            'id_venta','fecha','id_cliente','id_producto','cantidad',
            'precio_unitario','canal','ciudad','vendedor',
            'email_cliente','telefono_cliente','fecha_carga','archivo_origen'
        ])

        total = len(df)
        quality_checks = []

        # ── CHECK 1: Nulos en campos requeridos ─────────────
        required_fields = ['id_venta','fecha','id_cliente','id_producto',
                           'cantidad','precio_unitario','canal','ciudad',
                           'email_cliente','telefono_cliente']
        for campo in required_fields:
            invalidos = df[df[campo].apply(
                lambda x: pd.isna(x) or str(x).strip() == ''
            )]
            validos = total - len(invalidos)
            log_quality(hook, 'staging.ventas_raw', f'COMPLETENESS:{campo}',
                        total, validos, len(invalidos),
                        f"Nulos en {campo}: {list(invalidos['id_venta'].values)}" if len(invalidos) > 0 else "OK",
                        batch_id)
            quality_checks.append(validos / total if total > 0 else 1)

        # ── CHECK 2: Fechas válidas ──────────────────────────
        df['fecha_parsed'] = df['fecha'].apply(normalize_date)
        bad_dates = df[df['fecha_parsed'].isna()]
        validos_fecha = total - len(bad_dates)
        log_quality(hook, 'staging.ventas_raw', 'VALIDITY:fecha',
                    total, validos_fecha, len(bad_dates),
                    f"Fechas inválidas: {list(bad_dates['fecha'].values)}" if len(bad_dates) > 0 else "OK",
                    batch_id)
        quality_checks.append(validos_fecha / total if total > 0 else 1)

        # ── CHECK 3: Cantidad > 0 ────────────────────────────
        df['cantidad_num'] = pd.to_numeric(df['cantidad'], errors='coerce')
        bad_qty = df[df['cantidad_num'].isna() | (df['cantidad_num'] < 1)]
        validos_qty = total - len(bad_qty)
        log_quality(hook, 'staging.ventas_raw', 'VALIDITY:cantidad',
                    total, validos_qty, len(bad_qty),
                    f"Cantidades inválidas: {list(bad_qty[['id_venta','cantidad']].values)}" if len(bad_qty) > 0 else "OK",
                    batch_id)
        quality_checks.append(validos_qty / total if total > 0 else 1)

        # ── CHECK 4: precio_unitario > 0 ────────────────────
        df['precio_num'] = pd.to_numeric(df['precio_unitario'], errors='coerce')
        bad_price = df[df['precio_num'].isna() | (df['precio_num'] <= 0)]
        validos_price = total - len(bad_price)
        log_quality(hook, 'staging.ventas_raw', 'VALIDITY:precio_unitario',
                    total, validos_price, len(bad_price),
                    f"Precios inválidos: {list(bad_price['id_venta'].values)}" if len(bad_price) > 0 else "OK",
                    batch_id)
        quality_checks.append(validos_price / total if total > 0 else 1)

        # ── CHECK 5: Canal en lista permitida ───────────────
        canales_validos = list(CANAL_MAP.keys()) + list(CANAL_MAP.values())
        bad_canal = df[~df['canal'].apply(
            lambda x: str(x).strip().lower() in CANAL_MAP.keys()
        )]
        validos_canal = total - len(bad_canal)
        log_quality(hook, 'staging.ventas_raw', 'VALIDITY:canal',
                    total, validos_canal, len(bad_canal),
                    f"Canales no reconocidos: {list(bad_canal['canal'].values)}" if len(bad_canal) > 0 else "OK",
                    batch_id)
        quality_checks.append(validos_canal / total if total > 0 else 1)

        # ── CHECK 6: Duplicados internos ─────────────────────
        dups = df[df['id_venta'].duplicated(keep=False)]
        validos_dup = total - len(dups)
        log_quality(hook, 'staging.ventas_raw', 'UNIQUENESS:id_venta',
                    total, validos_dup, len(dups),
                    f"id_venta duplicados: {list(dups['id_venta'].values)}" if len(dups) > 0 else "OK",
                    batch_id)
        quality_checks.append(validos_dup / total if total > 0 else 1)

        # ── CHECK 7: Duplicados con fact_ventas (incremental) ─
        mode = context['ti'].xcom_pull(key='mode')
        if mode == 'incremental':
            existing_ids = hook.get_records(
                "SELECT id_venta FROM dwh.fact_ventas"
            )
            existing_set = {str(r[0]) for r in existing_ids}
            df_ids       = set(df['id_venta'].astype(str).values)
            cross_dups   = df_ids & existing_set
            validos_cross = total - len(cross_dups)
            log_quality(hook, 'staging.ventas_raw', 'UNIQUENESS:cross_file',
                        total, validos_cross, len(cross_dups),
                        f"IDs ya en fact_ventas: {cross_dups}" if cross_dups else "OK",
                        batch_id)
            quality_checks.append(validos_cross / total if total > 0 else 1)
            context['ti'].xcom_push(key='existing_ids', value=list(existing_set))

        # ── SCORE GLOBAL ─────────────────────────────────────
        score = round(sum(quality_checks) / len(quality_checks), 4)
        threshold = float(Variable.get('quality_threshold',
                           default_var=str(QUALITY_THRESH)))

        context['ti'].xcom_push(key='quality_score', value=score)
        logger.info(f"[VALIDATE] Score de calidad: {score*100:.1f}% (umbral: {threshold*100:.0f}%)")

        log_etl_end(hook, log_id, 'EXITOSO', total, total, 0)

        if score < threshold:
            msg = f"Score {score*100:.1f}% por debajo del umbral {threshold*100:.0f}%"
            logger.error(f"[VALIDATE] {msg}")
            raise ValueError(msg)

        logger.info(f"[VALIDATE] Validación aprobada – continuando pipeline")

    except Exception as e:
        log_etl_end(hook, log_id, 'FALLIDO', 0, 0, 0, str(e))
        logger.error(f"[VALIDATE] Error: {e}")
        raise


# ================================================================
# TASK 3: LIMPIEZA Y TRANSFORMACIÓN
# ================================================================

def clean_and_transform(**context):
    """
    Aplica todas las transformaciones sobre staging.ventas_raw
    y carga los datos limpios a staging.ventas_clean.

    Transformaciones:
        1. Normalizar fechas (YYYY/MM/DD → YYYY-MM-DD)
        2. Estandarizar canal (capitalización)
        3. Convertir tipos numéricos
        4. Manejar vendedor vacío → SIN_ASIGNAR
        5. Calcular monto_total = cantidad × precio_unitario
        6. Descartar registros con errores críticos
        7. Eliminar duplicados por id_venta
    """
    batch_id = context['ti'].xcom_pull(key='batch_id')
    mode     = context['ti'].xcom_pull(key='mode')
    hook     = get_pg_hook()
    log_id   = log_etl_start(hook, 'clean_and_transform', batch_id)

    logger.info(f"[TRANSFORM] Iniciando – Batch: {batch_id}")

    try:
        # Leer desde staging.ventas_raw
        rows = hook.get_records("""
            SELECT id_venta, fecha, id_cliente, id_producto,
                   cantidad, precio_unitario, canal, ciudad,
                   vendedor, email_cliente, telefono_cliente
            FROM staging.ventas_raw
            WHERE archivo_origen IN ('ventas_raw.csv','ventas_nuevas.csv')
            ORDER BY fecha_carga
        """)

        df = pd.DataFrame(rows, columns=[
            'id_venta','fecha','id_cliente','id_producto','cantidad',
            'precio_unitario','canal','ciudad','vendedor',
            'email_cliente','telefono_cliente'
        ])

        total_raw  = len(df)
        rechazados = []

        # ── 1. Parsear fechas ────────────────────────────────
        df['fecha'] = df['fecha'].apply(normalize_date)
        mask_bad_date = df['fecha'].isna()
        rechazados += df[mask_bad_date][['id_venta']].assign(
            motivo='FECHA_INVALIDA').to_dict('records')
        df = df[~mask_bad_date].copy()

        # ── 2. Normalizar canal ──────────────────────────────
        df['canal'] = df['canal'].apply(normalize_canal)
        mask_bad_canal = df['canal'].isna()
        rechazados += df[mask_bad_canal][['id_venta']].assign(
            motivo='CANAL_INVALIDO').to_dict('records')
        df = df[~mask_bad_canal].copy()

        # ── 3. Convertir tipos numéricos ─────────────────────
        for col in ['id_venta','id_cliente','id_producto','cantidad']:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        df['precio_unitario'] = pd.to_numeric(df['precio_unitario'], errors='coerce')

        # ── 4. Rechazar precio nulo ──────────────────────────
        mask_no_price = df['precio_unitario'].isna() | (df['precio_unitario'] <= 0)
        rechazados += df[mask_no_price][['id_venta']].assign(
            motivo='PRECIO_INVALIDO').to_dict('records')
        df = df[~mask_no_price].copy()

        # ── 5. Rechazar cantidad inválida ────────────────────
        mask_bad_qty = df['cantidad'].isna() | (df['cantidad'] < 1)
        rechazados += df[mask_bad_qty][['id_venta']].assign(
            motivo='CANTIDAD_INVALIDA').to_dict('records')
        df = df[~mask_bad_qty].copy()

        # ── 6. Imputar vendedor vacío ────────────────────────
        df['vendedor'] = df['vendedor'].apply(
            lambda x: 'SIN_ASIGNAR' if (pd.isna(x) or str(x).strip() == '') else str(x).strip()
        )

        # ── 7. Limpiar strings ───────────────────────────────
        for col in ['ciudad','email_cliente','telefono_cliente']:
            df[col] = df[col].apply(
                lambda x: str(x).strip() if pd.notna(x) else None
            )

        # ── 8. Calcular monto_total ──────────────────────────
        df['cantidad']        = df['cantidad'].astype(int)
        df['id_venta']        = df['id_venta'].astype(int)
        df['id_cliente']      = df['id_cliente'].astype(int)
        df['id_producto']     = df['id_producto'].astype(int)
        df['monto_total']     = (df['cantidad'] * df['precio_unitario']).round(2)

        # ── 9. Eliminar duplicados (mantener primero) ────────
        df = df.drop_duplicates(subset=['id_venta'], keep='first')

        # ── 10. En modo incremental: excluir IDs ya en DWH ──
        if mode == 'incremental':
            existing_ids = context['ti'].xcom_pull(key='existing_ids') or []
            existing_set = set(int(x) for x in existing_ids if str(x).isdigit())
            mask_dup = df['id_venta'].isin(existing_set)
            rechazados += df[mask_dup][['id_venta']].assign(
                motivo='DUPLICADO_EN_DWH').to_dict('records')
            df = df[~mask_dup].copy()

        total_clean = len(df)
        total_rech  = len(rechazados)

        logger.info(f"[TRANSFORM] Raw: {total_raw} | Limpios: {total_clean} | Rechazados: {total_rech}")
        if rechazados:
            logger.warning(f"[TRANSFORM] Rechazados: {rechazados}")

        # ── Limpiar staging.ventas_clean del mismo batch ─────
        hook.run(
            "DELETE FROM staging.ventas_clean WHERE id_batch = %s",
            parameters=(batch_id,)
        )

        # ── Insertar en staging.ventas_clean ─────────────────
        for _, row in df.iterrows():
            hook.run("""
                INSERT INTO staging.ventas_clean (
                    id_venta, fecha, id_cliente, id_producto,
                    cantidad, precio_unitario, canal, ciudad,
                    vendedor, email_cliente, telefono_cliente, id_batch
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, parameters=(
                int(row['id_venta']),    row['fecha'],
                int(row['id_cliente']),  int(row['id_producto']),
                int(row['cantidad']),    float(row['precio_unitario']),
                row['canal'],            row['ciudad'],
                row['vendedor'],         row.get('email_cliente'),
                row.get('telefono_cliente'), batch_id
            ))

        context['ti'].xcom_push(key='rows_clean', value=total_clean)
        log_etl_end(hook, log_id, 'EXITOSO', total_raw, total_clean, total_rech)
        logger.info(f"[TRANSFORM] Completado – {total_clean} registros en staging.ventas_clean")

    except Exception as e:
        log_etl_end(hook, log_id, 'FALLIDO', 0, 0, 0, str(e))
        logger.error(f"[TRANSFORM] Error: {e}")
        raise


# ================================================================
# TASK 4: CARGA DE DIMENSIONES
# ================================================================

def load_dimensions(**context):
    """
    Carga y actualiza todas las dimensiones desde staging.ventas_clean.

    dim_cliente  → SCD Type 2 (detecta cambios en ciudad/email/teléfono)
    dim_producto → upsert SCD Type 1
    dim_vendedor → upsert SCD Type 1
    dim_canal    → solo inserta si no existe
    dim_ciudad   → solo inserta si no existe
    dim_fecha    → ya está pre-poblada (2020-2030)
    """
    batch_id = context['ti'].xcom_pull(key='batch_id')
    hook     = get_pg_hook()
    log_id   = log_etl_start(hook, 'load_dimensions', batch_id)

    logger.info(f"[DIMENSIONS] Iniciando – Batch: {batch_id}")

    try:
        rows = hook.get_records("""
            SELECT id_venta, fecha, id_cliente, id_producto,
                   cantidad, precio_unitario, canal, ciudad,
                   vendedor, email_cliente, telefono_cliente, monto_total
            FROM staging.ventas_clean
            WHERE id_batch = %s
        """, parameters=(batch_id,))

        df = pd.DataFrame(rows, columns=[
            'id_venta','fecha','id_cliente','id_producto','cantidad',
            'precio_unitario','canal','ciudad','vendedor',
            'email_cliente','telefono_cliente','monto_total'
        ])

        # ── DIM_CANAL ────────────────────────────────────────
        for canal in df['canal'].unique():
            hook.run("""
                INSERT INTO dwh.dim_canal (nombre_canal, tipo_canal)
                VALUES (%s, %s)
                ON CONFLICT (nombre_canal) DO NOTHING
            """, parameters=(canal, 'Digital' if canal != 'Tienda' else 'Físico'))
        logger.info("[DIMENSIONS] dim_canal actualizada")

        # ── DIM_CIUDAD ───────────────────────────────────────
        for ciudad in df['ciudad'].unique():
            hook.run("""
                INSERT INTO dwh.dim_ciudad (nombre_ciudad)
                VALUES (%s)
                ON CONFLICT (nombre_ciudad) DO NOTHING
            """, parameters=(str(ciudad),))
        logger.info("[DIMENSIONS] dim_ciudad actualizada")

        # ── DIM_VENDEDOR ─────────────────────────────────────
        for vendedor in df['vendedor'].unique():
            hook.run("""
                INSERT INTO dwh.dim_vendedor (nombre_vendedor, estado)
                VALUES (%s, 'Activo')
                ON CONFLICT (nombre_vendedor) DO NOTHING
            """, parameters=(str(vendedor),))
        logger.info("[DIMENSIONS] dim_vendedor actualizada")

        # ── DIM_PRODUCTO ─────────────────────────────────────
        for prod_id in df['id_producto'].unique():
            hook.run("""
                INSERT INTO dwh.dim_producto
                    (id_producto, nombre_producto, categoria)
                VALUES (%s, %s, 'Sin Categoría')
                ON CONFLICT (id_producto) DO NOTHING
            """, parameters=(int(prod_id), f"Producto {int(prod_id)}"))
        logger.info("[DIMENSIONS] dim_producto actualizada")

        # ── DIM_CLIENTE (SCD Type 2) ─────────────────────────
        clientes_df = df[['id_cliente','email_cliente',
                           'telefono_cliente','ciudad']].drop_duplicates(
                           subset=['id_cliente'], keep='last')

        for _, cl in clientes_df.iterrows():
            id_cli = int(cl['id_cliente'])
            email  = str(cl['email_cliente']) if pd.notna(cl['email_cliente']) else None
            tel    = str(cl['telefono_cliente']) if pd.notna(cl['telefono_cliente']) else None
            ciudad = str(cl['ciudad'])

            # Verificar si ya existe registro activo
            existing = hook.get_first("""
                SELECT cliente_key, email_cliente, telefono_cliente, ciudad
                FROM dwh.dim_cliente
                WHERE id_cliente = %s AND es_registro_actual = TRUE
            """, parameters=(id_cli,))

            if existing is None:
                # Cliente nuevo → insertar
                hook.run("""
                    INSERT INTO dwh.dim_cliente
                        (id_cliente, email_cliente, telefono_cliente, ciudad,
                         fecha_vigencia_desde, fecha_vigencia_hasta, es_registro_actual)
                    VALUES (%s, %s, %s, %s, CURRENT_DATE, NULL, TRUE)
                """, parameters=(id_cli, email, tel, ciudad))

            else:
                # Cliente existente → verificar si cambió algo
                _, ex_email, ex_tel, ex_ciudad = existing
                changed = (
                    str(ex_email)  != str(email)  or
                    str(ex_tel)    != str(tel)     or
                    str(ex_ciudad) != str(ciudad)
                )
                if changed:
                    # SCD Type 2: cerrar registro anterior
                    hook.run("""
                        UPDATE dwh.dim_cliente
                        SET fecha_vigencia_hasta = CURRENT_DATE - INTERVAL '1 day',
                            es_registro_actual   = FALSE,
                            fecha_actualizacion  = CURRENT_TIMESTAMP
                        WHERE id_cliente = %s AND es_registro_actual = TRUE
                    """, parameters=(id_cli,))
                    # Insertar nuevo registro activo
                    hook.run("""
                        INSERT INTO dwh.dim_cliente
                            (id_cliente, email_cliente, telefono_cliente, ciudad,
                             fecha_vigencia_desde, fecha_vigencia_hasta, es_registro_actual)
                        VALUES (%s, %s, %s, %s, CURRENT_DATE, NULL, TRUE)
                    """, parameters=(id_cli, email, tel, ciudad))
                    logger.info(f"[DIMENSIONS] SCD2 – Cliente {id_cli} actualizado")

        logger.info("[DIMENSIONS] dim_cliente actualizada (SCD Type 2)")
        log_etl_end(hook, log_id, 'EXITOSO', len(df), len(df), 0)
        logger.info("[DIMENSIONS] Todas las dimensiones actualizadas correctamente")

    except Exception as e:
        log_etl_end(hook, log_id, 'FALLIDO', 0, 0, 0, str(e))
        logger.error(f"[DIMENSIONS] Error: {e}")
        raise


# ================================================================
# TASK 5: CARGA DE TABLA DE HECHOS
# ================================================================

def load_fact_table(**context):
    """
    Carga fact_ventas desde staging.ventas_clean.
    Realiza lookups a todas las dimensiones para obtener
    surrogate keys. Usa INSERT ON CONFLICT DO NOTHING para
    garantizar idempotencia.
    """
    batch_id = context['ti'].xcom_pull(key='batch_id')
    hook     = get_pg_hook()
    log_id   = log_etl_start(hook, 'load_fact_table', batch_id)

    logger.info(f"[FACT] Iniciando carga de hechos – Batch: {batch_id}")

    try:
        rows = hook.get_records("""
            SELECT id_venta, fecha, id_cliente, id_producto,
                   cantidad, precio_unitario, canal, ciudad,
                   vendedor, monto_total
            FROM staging.ventas_clean
            WHERE id_batch = %s
        """, parameters=(batch_id,))

        df = pd.DataFrame(rows, columns=[
            'id_venta','fecha','id_cliente','id_producto',
            'cantidad','precio_unitario','canal','ciudad',
            'vendedor','monto_total'
        ])

        exitosos  = 0
        fallidos  = 0
        fallidos_detalle = []

        for _, row in df.iterrows():
            try:
                id_venta    = int(row['id_venta'])
                fecha_obj   = row['fecha']
                id_cliente  = int(row['id_cliente'])
                id_producto = int(row['id_producto'])

                # ── Lookup fecha_key ─────────────────────────
                fecha_key = hook.get_first("""
                    SELECT fecha_key FROM dwh.dim_fecha WHERE fecha = %s
                """, parameters=(fecha_obj,))
                if not fecha_key:
                    raise ValueError(f"fecha_key no encontrado para {fecha_obj}")
                fecha_key = fecha_key[0]

                # ── Lookup cliente_key (registro actual) ─────
                cliente_key = hook.get_first("""
                    SELECT cliente_key FROM dwh.dim_cliente
                    WHERE id_cliente = %s AND es_registro_actual = TRUE
                """, parameters=(id_cliente,))
                if not cliente_key:
                    raise ValueError(f"cliente_key no encontrado para id_cliente={id_cliente}")
                cliente_key = cliente_key[0]

                # ── Lookup producto_key ──────────────────────
                producto_key = hook.get_first("""
                    SELECT producto_key FROM dwh.dim_producto
                    WHERE id_producto = %s
                """, parameters=(id_producto,))
                if not producto_key:
                    raise ValueError(f"producto_key no encontrado para id_producto={id_producto}")
                producto_key = producto_key[0]

                # ── Lookup vendedor_key ──────────────────────
                vendedor_key = hook.get_first("""
                    SELECT vendedor_key FROM dwh.dim_vendedor
                    WHERE nombre_vendedor = %s
                """, parameters=(str(row['vendedor']),))
                if not vendedor_key:
                    raise ValueError(f"vendedor_key no encontrado para {row['vendedor']}")
                vendedor_key = vendedor_key[0]

                # ── Lookup canal_key ─────────────────────────
                canal_key = hook.get_first("""
                    SELECT canal_key FROM dwh.dim_canal
                    WHERE nombre_canal = %s
                """, parameters=(str(row['canal']),))
                if not canal_key:
                    raise ValueError(f"canal_key no encontrado para {row['canal']}")
                canal_key = canal_key[0]

                # ── Lookup ciudad_key ────────────────────────
                ciudad_key = hook.get_first("""
                    SELECT ciudad_key FROM dwh.dim_ciudad
                    WHERE nombre_ciudad = %s
                """, parameters=(str(row['ciudad']),))
                if not ciudad_key:
                    raise ValueError(f"ciudad_key no encontrado para {row['ciudad']}")
                ciudad_key = ciudad_key[0]

                # ── INSERT en fact_ventas (idempotente) ──────
                hook.run("""
                    INSERT INTO dwh.fact_ventas (
                        id_venta, fecha_key, cliente_key, producto_key,
                        vendedor_key, canal_key, ciudad_key,
                        cantidad, precio_unitario, monto_total,
                        descuento, impuestos, fecha_venta, id_batch
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,0,0,%s,%s)
                    ON CONFLICT (id_venta) DO NOTHING
                """, parameters=(
                    id_venta,     fecha_key,    cliente_key,  producto_key,
                    vendedor_key, canal_key,    ciudad_key,
                    int(row['cantidad']), float(row['precio_unitario']),
                    float(row['monto_total']), fecha_obj, batch_id
                ))
                exitosos += 1

            except Exception as row_err:
                fallidos += 1
                fallidos_detalle.append(f"id_venta={row['id_venta']}: {row_err}")
                logger.warning(f"[FACT] Registro omitido – {row_err}")

        total = exitosos + fallidos
        context['ti'].xcom_push(key='rows_fact', value=exitosos)

        if fallidos_detalle:
            logger.warning(f"[FACT] Registros fallidos: {fallidos_detalle}")

        log_etl_end(hook, log_id,
                    'EXITOSO' if fallidos == 0 else 'PARCIAL',
                    total, exitosos, fallidos,
                    str(fallidos_detalle) if fallidos_detalle else None)

        logger.info(f"[FACT] Completado – Exitosos: {exitosos} | Fallidos: {fallidos}")

    except Exception as e:
        log_etl_end(hook, log_id, 'FALLIDO', 0, 0, 0, str(e))
        logger.error(f"[FACT] Error crítico: {e}")
        raise


# ================================================================
# TASK 6: LIMPIEZA DE STAGING
# ================================================================

def cleanup_staging(**context):
    """
    Elimina registros antiguos de staging según política
    de retención configurada en Variable 'retention_days'.
    Por defecto: 7 días.
    """
    batch_id       = context['ti'].xcom_pull(key='batch_id')
    retention_days = int(Variable.get('retention_days',
                          default_var=str(RETENTION_DAYS)))
    hook           = get_pg_hook()
    log_id         = log_etl_start(hook, 'cleanup_staging', batch_id)

    logger.info(f"[CLEANUP] Iniciando – Retención: {retention_days} días")

    try:
        # Eliminar staging_raw más antiguo que retention_days
        r1 = hook.run("""
            DELETE FROM staging.ventas_raw
            WHERE fecha_carga < NOW() - INTERVAL '%s days'
        """ % retention_days)

        # Eliminar staging_clean más antiguo que retention_days
        r2 = hook.run("""
            DELETE FROM staging.ventas_clean
            WHERE fecha_procesamiento < NOW() - INTERVAL '%s days'
        """ % retention_days)

        log_etl_end(hook, log_id, 'EXITOSO', 0, 0, 0)
        logger.info(f"[CLEANUP] Completado – staging limpiado correctamente")

    except Exception as e:
        log_etl_end(hook, log_id, 'FALLIDO', 0, 0, 0, str(e))
        logger.error(f"[CLEANUP] Error: {e}")
        raise


# ================================================================
# TASK 7: NOTIFICACIÓN DE ÉXITO
# ================================================================

def send_success_notification(**context):
    """
    Registra resumen final del pipeline en los logs.
    En producción aquí iría envío de email o mensaje a Slack.
    """
    batch_id     = context['ti'].xcom_pull(key='batch_id')
    rows_ext     = context['ti'].xcom_pull(key='rows_extracted') or 0
    rows_clean   = context['ti'].xcom_pull(key='rows_clean')     or 0
    rows_fact    = context['ti'].xcom_pull(key='rows_fact')      or 0
    quality_score= context['ti'].xcom_pull(key='quality_score')  or 0
    mode         = context['ti'].xcom_pull(key='mode')           or 'initial'

    summary = f"""
    ╔══════════════════════════════════════════╗
    ║   ETL VENTAS ASISYA – PIPELINE EXITOSO   ║
    ╠══════════════════════════════════════════╣
    ║  Batch ID     : {batch_id}
    ║  Modo         : {mode.upper()}
    ║  Extraídos    : {rows_ext}
    ║  Limpios      : {rows_clean}
    ║  En fact_ventas: {rows_fact}
    ║  Score calidad: {quality_score*100:.1f}%
    ╚══════════════════════════════════════════╝
    """
    logger.info(summary)

    # TODO producción: enviar email o webhook Slack
    # requests.post(SLACK_WEBHOOK, json={"text": summary})


# ================================================================
# DEFINICIÓN DEL DAG
# ================================================================

with DAG(
    dag_id          = 'etl_ventas_asisya',
    default_args    = default_args,
    description     = 'Pipeline ETL de ventas – ASISYA (carga inicial e incremental)',
    schedule_interval= '0 2 * * *',   # Diario a las 2 AM
    start_date      = days_ago(1),
    catchup         = False,
    tags            = ['ventas', 'etl', 'dwh', 'asisya'],
    max_active_runs = 1,
    params          = {
        'mode': 'initial',   # 'initial' | 'incremental'
    }
) as dag:

    t1_extract = PythonOperator(
        task_id         = 'extract_from_csv',
        python_callable = extract_from_csv,
        provide_context = True,
    )

    t2_validate = PythonOperator(
        task_id         = 'profile_and_validate',
        python_callable = profile_and_validate,
        provide_context = True,
    )

    t3_transform = PythonOperator(
        task_id         = 'clean_and_transform',
        python_callable = clean_and_transform,
        provide_context = True,
    )

    t4_dimensions = PythonOperator(
        task_id         = 'load_dimensions',
        python_callable = load_dimensions,
        provide_context = True,
    )

    t5_fact = PythonOperator(
        task_id         = 'load_fact_table',
        python_callable = load_fact_table,
        provide_context = True,
    )

    t6_cleanup = PythonOperator(
        task_id         = 'cleanup_staging',
        python_callable = cleanup_staging,
        provide_context = True,
    )

    t7_notify = PythonOperator(
        task_id         = 'send_success_notification',
        python_callable = send_success_notification,
        provide_context = True,
    )

    # Dependencias lineales
    t1_extract >> t2_validate >> t3_transform >> t4_dimensions >> t5_fact >> t6_cleanup >> t7_notify
