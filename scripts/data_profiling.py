"""
=============================================================
FASE 1 - Data Profiling y Calidad de Datos
Prueba Técnica Data Engineer - ASISYA
=============================================================


"""

import pandas as pd
import numpy as np
import json
import re
import os
from datetime import datetime, date

# ─────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────
DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
RAW_FILE  = os.path.join(DATA_DIR, 'ventas_raw.csv')
NEW_FILE  = os.path.join(DATA_DIR, 'ventas_nuevas.csv')

BATCH_ID  = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

QUALITY_THRESHOLD = 0.75   

# ─────────────────────────────────────────────
# REGLAS DE NEGOCIO POR CAMPO
# ─────────────────────────────────────────────
FIELD_RULES = {
    'id_venta':         {'required': True,  'type': 'int',   'unique': True,  'min': 1},
    'fecha':            {'required': True,  'type': 'date',  'min_date': '2020-01-01', 'max_date': '2030-12-31'},
    'id_cliente':       {'required': True,  'type': 'int',   'min': 1},
    'id_producto':      {'required': True,  'type': 'int',   'min': 1},
    'cantidad':         {'required': True,  'type': 'int',   'min': 1},
    'precio_unitario':  {'required': True,  'type': 'float', 'min': 0.01},
    'canal':            {'required': True,  'type': 'str',   'allowed': ['Online', 'Tienda', 'Marketplace']},
    'ciudad':           {'required': True,  'type': 'str'},
    'vendedor':         {'required': False, 'type': 'str'},
    'email_cliente':    {'required': True,  'type': 'email'},
    'telefono_cliente': {'required': True,  'type': 'str',   'pattern': r'^\d{10}$'},
}

# ─────────────────────────────────────────────
# FUNCIONES AUXILIARES
# ─────────────────────────────────────────────

def normalize_date(val):
    """Intenta parsear fechas en formatos mixtos. Retorna date o None."""
    if pd.isna(val):
        return None
    val = str(val).strip()
    for fmt in ('%Y-%m-%d', '%Y/%m/%d', '%d-%m-%Y', '%d/%m/%Y'):
        try:
            return datetime.strptime(val, fmt).date()
        except ValueError:
            continue
    return None


def is_valid_email(val):
    """Valida formato básico de email."""
    if pd.isna(val):
        return False
    pattern = r'^[\w\.-]+@[\w\.-]+\.\w{2,}$'
    return bool(re.match(pattern, str(val).strip()))


def normalize_canal(val):
    """Estandariza el canal a los valores permitidos."""
    if pd.isna(val):
        return None
    mapping = {
        'online':      'Online',
        'tienda':      'Tienda',
        'marketplace': 'Marketplace',
    }
    return mapping.get(str(val).strip().lower(), str(val).strip())


# ─────────────────────────────────────────────
# CLASE PRINCIPAL DE PROFILING
# ─────────────────────────────────────────────

class DataProfiler:
    def __init__(self, filepath: str, batch_id: str):
        self.filepath  = filepath
        self.batch_id  = batch_id
        self.filename  = os.path.basename(filepath)
        self.df_raw    = pd.read_csv(filepath, dtype=str)   
        self.issues    = []          
        self.quality_log = []        

    # ── 1. ESTADÍSTICAS DESCRIPTIVAS ────────────────────────────────

    def basic_stats(self):
        df = self.df_raw
        stats = {
            'total_registros':   len(df),
            'total_columnas':    len(df.columns),
            'columnas':          list(df.columns),
            'duplicados_id_venta': int(df['id_venta'].duplicated().sum()),
            'filas_completamente_duplicadas': int(df.duplicated().sum()),
        }

        col_stats = {}
        for col in df.columns:
            series = df[col]
            nulos  = int(series.isna().sum() + (series == '').sum())
            col_stats[col] = {
                'nulos':           nulos,
                'pct_nulos':       round(nulos / len(df) * 100, 2),
                'distintos':       int(series.nunique()),
                'valor_mas_comun': series.mode()[0] if not series.mode().empty else None,
            }
            # Estadísticas numéricas donde aplique
            try:
                num = pd.to_numeric(series, errors='coerce')
                if num.notna().sum() > 0:
                    col_stats[col].update({
                        'min': float(num.min()),
                        'max': float(num.max()),
                        'media': round(float(num.mean()), 2),
                        'mediana': float(num.median()),
                    })
            except Exception:
                pass

        stats['por_columna'] = col_stats
        return stats

    # ── 2. VALIDACIONES POR REGLA ────────────────────────────────────

    def validate_nulls(self):
        """Valida campos requeridos sin valor."""
        df = self.df_raw
        for col, rules in FIELD_RULES.items():
            if col not in df.columns:
                continue
            if rules.get('required'):
                mask = df[col].isna() | (df[col].strip() == '' if df[col].dtype == object else False)
                # Usar apply para evitar error en series de strings
                mask = df[col].apply(lambda x: pd.isna(x) or str(x).strip() == '')
                bad  = df[mask]
                if not bad.empty:
                    self.issues.append({
                        'tipo':    'NULO_EN_CAMPO_REQUERIDO',
                        'campo':   col,
                        'count':   len(bad),
                        'ids':     list(bad['id_venta'].values),
                        'detalle': f"Campo obligatorio '{col}' con {len(bad)} valores vacíos",
                    })
                self._log_quality('NULLS', col,
                                  len(df), len(df) - len(bad), len(bad))

    def validate_dates(self):
        """Valida fechas: formato, rango y valores imposibles."""
        df   = self.df_raw
        col  = 'fecha'
        parsed   = df[col].apply(normalize_date)
        invalids = df[parsed.isna()]

        if not invalids.empty:
            self.issues.append({
                'tipo':    'FECHA_INVALIDA',
                'campo':   col,
                'count':   len(invalids),
                'ids':     list(invalids['id_venta'].values),
                'detalle': f"Fechas que no pudieron parsearse: {list(invalids[col].values)}",
            })

        # Fechas fuera de rango lógico
        rule   = FIELD_RULES[col]
        minD   = datetime.strptime(rule['min_date'], '%Y-%m-%d').date()
        maxD   = datetime.strptime(rule['max_date'], '%Y-%m-%d').date()
        out_of_range = parsed.apply(lambda d: d is not None and not (minD <= d <= maxD))
        bad_range = df[out_of_range]
        if not bad_range.empty:
            self.issues.append({
                'tipo':    'FECHA_FUERA_RANGO',
                'campo':   col,
                'count':   len(bad_range),
                'ids':     list(bad_range['id_venta'].values),
                'detalle': f"Fechas fuera del rango {rule['min_date']} – {rule['max_date']}",
            })

        validos = len(df) - len(invalids) - len(bad_range)
        self._log_quality('DATE_FORMAT', col, len(df), validos, len(invalids) + len(bad_range))

    def validate_numerics(self):
        """Valida campos numéricos: tipo, mínimo."""
        df = self.df_raw
        for col in ['id_venta', 'id_cliente', 'id_producto', 'cantidad', 'precio_unitario']:
            if col not in df.columns:
                continue
            num = pd.to_numeric(df[col], errors='coerce')
            not_numeric = df[num.isna() & df[col].notna() & (df[col].str.strip() != '')]
            min_val = FIELD_RULES[col].get('min', None)
            too_low = df[num.notna() & (num < min_val)] if min_val is not None else pd.DataFrame()

            if not not_numeric.empty:
                self.issues.append({
                    'tipo':    'TIPO_DATO_INVALIDO',
                    'campo':   col,
                    'count':   len(not_numeric),
                    'ids':     list(not_numeric['id_venta'].values),
                    'detalle': f"Valores no numéricos en '{col}'",
                })
            if not too_low.empty:
                self.issues.append({
                    'tipo':    'VALOR_FUERA_RANGO',
                    'campo':   col,
                    'count':   len(too_low),
                    'ids':     list(too_low['id_venta'].values),
                    'detalle': f"'{col}' tiene valores < {min_val}: {list(too_low[col].values)}",
                })
            invalidos = len(not_numeric) + len(too_low)
            self._log_quality('NUMERIC_RANGE', col, len(df), len(df) - invalidos, invalidos)

    def validate_canal(self):
        """Valida que el canal sea uno de los valores permitidos (case-insensitive)."""
        df      = self.df_raw
        col     = 'canal'
        allowed = [v.lower() for v in FIELD_RULES[col]['allowed']]
        mask    = df[col].apply(lambda x: str(x).strip().lower() not in allowed if pd.notna(x) and str(x).strip() != '' else False)
        bad     = df[mask]

        # Inconsistencias de capitalización (no errores, pero sí sucios)
        wrong_case = df[df[col].apply(
            lambda x: str(x).strip().lower() in allowed and str(x).strip() not in FIELD_RULES[col]['allowed']
        )]
        if not wrong_case.empty:
            self.issues.append({
                'tipo':    'CANAL_CAPITALIZACIÓN_INCORRECTA',
                'campo':   col,
                'count':   len(wrong_case),
                'ids':     list(wrong_case['id_venta'].values),
                'detalle': f"Canal con capitalización incorrecta: {list(wrong_case[col].values)}",
            })
        if not bad.empty:
            self.issues.append({
                'tipo':    'CANAL_VALOR_INVALIDO',
                'campo':   col,
                'count':   len(bad),
                'ids':     list(bad['id_venta'].values),
                'detalle': f"Valores de canal no reconocidos: {list(bad[col].values)}",
            })
        invalidos = len(bad)
        self._log_quality('CANAL_VALUES', col, len(df), len(df) - invalidos, invalidos)

    def validate_emails(self):
        """Valida formato de email."""
        df  = self.df_raw
        col = 'email_cliente'
        bad = df[~df[col].apply(is_valid_email)]
        if not bad.empty:
            self.issues.append({
                'tipo':    'EMAIL_INVALIDO',
                'campo':   col,
                'count':   len(bad),
                'ids':     list(bad['id_venta'].values),
                'detalle': f"Emails con formato inválido o vacíos: {list(bad[col].values)}",
            })
        self._log_quality('EMAIL_FORMAT', col, len(df), len(df) - len(bad), len(bad))

    def validate_duplicates(self):
        """Detecta duplicados por id_venta entre raw y nuevas."""
        df  = self.df_raw
        col = 'id_venta'
        dups = df[df[col].duplicated(keep=False)]
        if not dups.empty:
            self.issues.append({
                'tipo':    'DUPLICADO_ID_VENTA',
                'campo':   col,
                'count':   len(dups),
                'ids':     list(dups[col].values),
                'detalle': f"id_venta duplicado dentro del mismo archivo",
            })
        self._log_quality('DUPLICATES', col, len(df), len(df) - len(dups), len(dups))

    def cross_file_duplicates(self, df_new: pd.DataFrame):
        """Detecta ids de ventas_nuevas que ya existen en ventas_raw."""
        ids_raw = set(self.df_raw['id_venta'].dropna().values)
        ids_new = set(df_new['id_venta'].dropna().values)
        overlap = ids_raw & ids_new
        if overlap:
            self.issues.append({
                'tipo':    'DUPLICADO_ENTRE_ARCHIVOS',
                'campo':   'id_venta',
                'count':   len(overlap),
                'ids':     list(overlap),
                'detalle': f"id_venta presentes en AMBOS archivos (duplicados en carga incremental): {overlap}",
            })

    # ── 3. DETECCIÓN DE OUTLIERS ─────────────────────────────────────

    def detect_outliers(self):
        """Usa IQR para detectar outliers en cantidad y precio_unitario."""
        df = self.df_raw
        for col in ['cantidad', 'precio_unitario']:
            num = pd.to_numeric(df[col], errors='coerce').dropna()
            if len(num) < 4:
                continue
            Q1, Q3 = num.quantile(0.25), num.quantile(0.75)
            IQR    = Q3 - Q1
            lower  = Q1 - 1.5 * IQR
            upper  = Q3 + 1.5 * IQR
            outliers = df[pd.to_numeric(df[col], errors='coerce').apply(
                lambda x: pd.notna(x) and (x < lower or x > upper)
            )]
            if not outliers.empty:
                self.issues.append({
                    'tipo':    'OUTLIER_DETECTADO',
                    'campo':   col,
                    'count':   len(outliers),
                    'ids':     list(outliers['id_venta'].values),
                    'detalle': f"Outliers en '{col}' (rango esperado [{lower:.0f}, {upper:.0f}]): {list(outliers[col].values)}",
                })

    # ── 4. CALIDAD GLOBAL ────────────────────────────────────────────

    def compute_quality_score(self):
        """Calcula score global de calidad 0-1."""
        if not self.quality_log:
            return 0.0
        total_eval  = sum(r['registros_evaluados'] for r in self.quality_log)
        total_valid = sum(r['registros_validos']   for r in self.quality_log)
        return round(total_valid / total_eval, 4) if total_eval > 0 else 0.0

    def _log_quality(self, regla, campo, evaluados, validos, invalidos):
        pct = round(validos / evaluados * 100, 2) if evaluados > 0 else 0
        self.quality_log.append({
            'tabla_origen':        self.filename,
            'regla_calidad':       f"{regla}:{campo}",
            'fecha_validacion':    datetime.now().isoformat(),
            'registros_evaluados': evaluados,
            'registros_validos':   validos,
            'registros_invalidos': invalidos,
            'porcentaje_calidad':  pct,
            'id_batch':            self.batch_id,
        })

    # ── 5. REPORTE COMPLETO ──────────────────────────────────────────

    def run_full_profile(self, df_new: pd.DataFrame = None):
        print(f"\n{'='*60}")
        print(f"  DATA PROFILING - {self.filename}")
        print(f"  Batch ID: {self.batch_id}")
        print(f"{'='*60}")

        stats = self.basic_stats()
        self.validate_nulls()
        self.validate_dates()
        self.validate_numerics()
        self.validate_canal()
        self.validate_emails()
        self.validate_duplicates()
        self.detect_outliers()
        if df_new is not None:
            self.cross_file_duplicates(df_new)

        score = self.compute_quality_score()

        print(f"\n ESTADÍSTICAS GENERALES")
        print(f"   Total registros   : {stats['total_registros']}")
        print(f"   Total columnas    : {stats['total_columnas']}")
        print(f"   Duplicados id_venta: {stats['duplicados_id_venta']}")
        print(f"   Filas duplicadas  : {stats['filas_completamente_duplicadas']}")

        print(f"\n NULOS POR COLUMNA")
        for col, cs in stats['por_columna'].items():
            indicator = '⚠️ ' if cs['nulos'] > 0 else '✅'
            print(f"   {indicator} {col:<25} nulos: {cs['nulos']} ({cs['pct_nulos']}%)")

        print(f"\n🚨 PROBLEMAS DETECTADOS ({len(self.issues)})")
        for i, issue in enumerate(self.issues, 1):
            print(f"\n   [{i}] {issue['tipo']}")
            print(f"       Campo   : {issue['campo']}")
            print(f"       Cantidad: {issue['count']}")
            print(f"       IDs     : {issue['ids']}")
            print(f"       Detalle : {issue['detalle']}")

        print(f"\n SCORE DE CALIDAD GLOBAL: {score*100:.1f}%")
        status = 'ACEPTABLE' if score >= QUALITY_THRESHOLD else '❌ POR DEBAJO DEL UMBRAL'
        print(f"   Estado  : {status} (umbral: {QUALITY_THRESHOLD*100:.0f}%)")
        print(f"\n{'='*60}\n")

        return {
            'stats':        stats,
            'issues':       self.issues,
            'quality_log':  self.quality_log,
            'quality_score': score,
            'batch_id':     self.batch_id,
            'pass_threshold': score >= QUALITY_THRESHOLD,
        }


# ─────────────────────────────────────────────
# ESTRATEGIA DE LIMPIEZA (para el ETL)
# ─────────────────────────────────────────────

def clean_dataframe(df_raw: pd.DataFrame) -> tuple[pd.DataFrame, list]:
    """
    Aplica todas las transformaciones de limpieza al DataFrame raw.
    Retorna (df_clean, lista_de_rechazados).
    """
    df      = df_raw.copy()
    rejected = []

    # 1. Normalizar fechas
    df['fecha_parsed'] = df['fecha'].apply(normalize_date)
    mask_bad_date = df['fecha_parsed'].isna()
    rejected += df[mask_bad_date][['id_venta', 'fecha']].assign(motivo='FECHA_INVALIDA').to_dict('records')
    df = df[~mask_bad_date].copy()
    df['fecha'] = df['fecha_parsed']
    df.drop(columns=['fecha_parsed'], inplace=True)

    # 2. Normalizar canal
    df['canal'] = df['canal'].apply(normalize_canal)

    # 3. Convertir tipos numéricos
    for col in ['id_venta', 'id_cliente', 'id_producto', 'cantidad']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    df['precio_unitario'] = pd.to_numeric(df['precio_unitario'], errors='coerce')

    # 4. Descartar filas con precio_unitario nulo (no imputar precio)
    mask_no_price = df['precio_unitario'].isna()
    rejected += df[mask_no_price][['id_venta']].assign(motivo='PRECIO_NULO').to_dict('records')
    df = df[~mask_no_price].copy()

    # 5. Descartar cantidad <= 0
    mask_bad_qty = df['cantidad'] <= 0
    rejected += df[mask_bad_qty][['id_venta', 'cantidad']].assign(motivo='CANTIDAD_INVALIDA').to_dict('records')
    df = df[~mask_bad_qty].copy()

    # 6. Calcular monto_total
    df['monto_total'] = (df['cantidad'] * df['precio_unitario']).round(2)

    # 7. Limpiar strings
    for col in ['ciudad', 'vendedor', 'email_cliente', 'telefono_cliente']:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: str(x).strip() if pd.notna(x) else None)

    # 8. Vendedor nulo → 'SIN_ASIGNAR'
    df['vendedor'] = df['vendedor'].apply(
        lambda x: 'SIN_ASIGNAR' if (pd.isna(x) or str(x).strip() == '') else x
    )

    # 9. Eliminar duplicados por id_venta (mantener primero)
    df = df.drop_duplicates(subset=['id_venta'], keep='first')

    # Convertir tipos finales
    df['id_venta']    = df['id_venta'].astype(int)
    df['id_cliente']  = df['id_cliente'].astype(int)
    df['id_producto'] = df['id_producto'].astype(int)
    df['cantidad']    = df['cantidad'].astype(int)

    return df, rejected


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

if __name__ == '__main__':
    df_raw = pd.read_csv(RAW_FILE, dtype=str)
    df_new = pd.read_csv(NEW_FILE, dtype=str)

    # ── Profiling del archivo principal
    profiler = DataProfiler(RAW_FILE, BATCH_ID)
    result   = profiler.run_full_profile(df_new=df_new)

    # ── Aplicar limpieza
    df_clean, rechazados = clean_dataframe(df_raw)

    print(f"📦 RESULTADO LIMPIEZA")
    print(f"   Registros originales : {len(df_raw)}")
    print(f"   Registros limpios    : {len(df_clean)}")
    print(f"   Registros rechazados : {len(rechazados)}")
    if rechazados:
        print(f"   Detalle rechazados   : {rechazados}")

    print(f"\n✅ Columnas del DataFrame limpio: {list(df_clean.columns)}")
    print(df_clean[['id_venta', 'fecha', 'canal', 'cantidad', 'precio_unitario', 'monto_total']].to_string(index=False))

    # ── Guardar quality_log como JSON (para simular INSERT a control.data_quality_log)
    log_path = os.path.join(os.path.dirname(__file__), '..', 'docs', 'quality_log.json')
    with open(log_path, 'w', encoding='utf-8') as f:
        json.dump(result['quality_log'], f, ensure_ascii=False, indent=2, default=str)
    print(f"\n💾 Quality log guardado en: {log_path}")
