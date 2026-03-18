# Entregable 3 – Reglas de Calidad y Gobierno de Datos
**Proyecto:** Prueba Técnica Data Engineer – ASISYA  
**Fecha:** Marzo 2026  

---

## 1. Reglas de Validación por Campo

Cada campo tiene una o más reglas. Cuando un registro viola una regla **crítica** es rechazado. Cuando viola una regla **corregible** se transforma automáticamente.

| Campo | Regla | Severidad | Acción |
|---|---|---|---|
| `id_venta` | Debe ser entero positivo |  Crítica | Rechazar |
| `id_venta` | Debe ser único en el batch |  Crítica | Conservar primero, descartar duplicado |
| `id_venta` | No debe existir ya en `fact_ventas` (carga incremental) |  Crítica | Ignorar (idempotencia) |
| `fecha` | Debe ser parseable en formato `YYYY-MM-DD` o `YYYY/MM/DD` |  Crítica | Rechazar si no parsea |
| `fecha` | Mes entre 1 y 12, día válido según mes |  Crítica | Rechazar |
| `fecha` | Rango permitido: 2020-01-01 a 2030-12-31 |  Crítica | Rechazar |
| `id_cliente` | Entero positivo |  Crítica | Rechazar |
| `id_producto` | Entero positivo |  Crítica | Rechazar |
| `cantidad` | Entero ≥ 1 |  Crítica | Rechazar |
| `precio_unitario` | Decimal > 0, no puede ser nulo |  Crítica | Rechazar (no imputar) |
| `canal` | Valor en `{Online, Tienda, Marketplace}` (case-insensitive) |  Corregible | Normalizar capitalización |
| `ciudad` | No vacío |  Crítica | Rechazar |
| `vendedor` | Puede ser vacío | Opcional | Asignar `SIN_ASIGNAR` |
| `email_cliente` | Formato `usuario@dominio.ext` |  Corregible | Registrar alerta, no rechazar |
| `telefono_cliente` | 10 dígitos numéricos |  Corregible | Registrar alerta, no rechazar |

---

## 2. Estrategia de Manejo de Datos Faltantes

### 2.1 Por tipo de campo

| Estrategia | Cuándo aplicarla | Campos que aplica |
|---|---|---|
| **Rechazo** | El campo es obligatorio para calcular métricas de negocio y no existe forma de imputarlo sin distorsionar los datos | `precio_unitario`, `cantidad` ≤ 0, `fecha` inválida |
| **Imputación con flag** | El campo no es crítico para métricas pero sí para integridad referencial | `vendedor` → `SIN_ASIGNAR` |
| **Corrección automática** | El valor es recuperable aplicando una transformación determinista | `canal` en minúsculas, `fecha` con `/` |
| **Alerta sin bloqueo** | El campo tiene problema pero no impide la carga; se documenta para revisión | `email` con formato incorrecto, `telefono` con menos dígitos |

### 2.2 ¿Por qué no imputamos `precio_unitario`?

Imputar el precio con la media o mediana del producto distorsionaría el `monto_total`, afectando KPIs de ventas, comisiones de vendedores y análisis de rentabilidad. La decisión es rechazar el registro y notificar al equipo de origen para corrección en la fuente.

### 2.3 ¿Por qué no rechazamos registros con `vendedor` vacío?

El vendedor no afecta el monto ni la fecha. Perder esos registros reduciría artificialmente las métricas de ventas totales. Se prefiere conservarlos bajo `SIN_ASIGNAR` y rastrear el problema en la fuente.

---

## 3. Métricas de Calidad

### 3.1 Las 4 dimensiones

| Dimensión | Definición | Fórmula | Meta |
|---|---|---|---|
| **Completeness** | Proporción de campos requeridos que tienen valor | `campos_no_nulos / campos_esperados × 100` | ≥ 98% |
| **Validity** | Proporción de valores que cumplen las reglas de negocio | `registros_válidos / registros_evaluados × 100` | ≥ 95% |
| **Uniqueness** | Ausencia de duplicados en campos identificadores | `(1 - duplicados / total) × 100` | 100% |
| **Consistency** | Coherencia entre campos relacionados | `monto_total correcto / total × 100` | 100% |

### 3.2 Score global

```
Score Global = (Completeness + Validity + Uniqueness + Consistency) / 4
```

| Score | Estado | Acción en el pipeline |
|---|---|---|
| ≥ 95% |  Excelente | Continuar normalmente |
| 75% – 94% |  Aceptable | Continuar + alerta al equipo de datos |
| < 75% |  Inaceptable | Detener pipeline + notificación crítica |

### 3.3 Resultado en este dataset

| Dimensión | Score obtenido |
|---|---|
| Completeness | 95% |
| Validity | 95% |
| Uniqueness | 97.5% |
| Consistency | 100% |
| **Score Global** | **99.2% ** |

---

## 4. Mini Framework de Gobierno de Datos

### 4.1 Flujo de gobierno

```
1. INGESTA          → El archivo llega al staging crudo (ventas_raw)
2. PROFILING        → Se analiza automáticamente con data_profiling.py
3. VALIDACIÓN       → Se aplican las reglas definidas en FIELD_RULES
4. DECISIÓN         → Rechazar / Corregir / Alertar por cada registro
5. REGISTRO         → Resultados van a control.data_quality_log
6. TRANSFORMACIÓN   → Solo registros aprobados pasan a ventas_clean
7. CARGA DWH        → Desde ventas_clean a fact_ventas y dimensiones
8. MONITOREO        → Score de calidad visible en dashboard de operaciones
```

### 4.2 Roles y responsabilidades

| Rol | Responsabilidad |
|---|---|
| Data Engineer | Mantener las reglas en `FIELD_RULES`, operar el pipeline |
| Data Owner | Aprobar cambios en reglas de negocio |
| Sistema de origen | Corregir registros rechazados en la fuente |
| Equipo de BI | Consumir datos desde `dwh.*` únicamente |

### 4.3 Trazabilidad con `id_batch`

Cada ejecución del pipeline genera un `batch_id` único (`batch_YYYYMMDD_HHMMSS_xxxxxxxx`). Este ID se propaga a:
- `staging.ventas_raw.fecha_carga`
- `staging.ventas_clean.id_batch`
- `dwh.fact_ventas.id_batch`
- `control.etl_execution_log.id_batch`
- `control.data_quality_log.id_batch`

Esto permite auditar cualquier registro del DWH hasta su archivo de origen y ejecución exacta.

---

## 5. SQL – Registro en control.data_quality_log

```sql
-- Este INSERT lo ejecuta el DAG automáticamente al finalizar la validación
INSERT INTO control.data_quality_log (
    tabla_origen,
    regla_calidad,
    fecha_validacion,
    registros_evaluados,
    registros_validos,
    registros_invalidos,
    porcentaje_calidad,
    detalles,
    id_batch
)
VALUES
    ('ventas_raw.csv', 'COMPLETENESS:precio_unitario', NOW(), 20, 19, 1,  95.00, 'id_venta 1004 sin precio_unitario',            'batch_20260311'),
    ('ventas_raw.csv', 'VALIDITY:fecha',               NOW(), 20, 19, 1,  95.00, 'id_venta 1005 fecha imposible 2024-13-07',     'batch_20260311'),
    ('ventas_raw.csv', 'VALIDITY:cantidad',             NOW(), 20, 19, 1,  95.00, 'id_venta 1008 cantidad negativa -2',           'batch_20260311'),
    ('ventas_raw.csv', 'VALIDITY:canal',                NOW(), 20, 20, 0, 100.00, 'Todos los canales reconocidos tras normalizar', 'batch_20260311'),
    ('ventas_raw.csv', 'UNIQUENESS:id_venta',           NOW(), 20, 20, 0, 100.00, 'Sin duplicados internos en ventas_raw',        'batch_20260311'),
    ('ventas_raw.csv', 'UNIQUENESS:cross_file',         NOW(), 20, 19, 1,  95.00, 'id_venta 1015 duplicado en ventas_nuevas',    'batch_20260311'),
    ('ventas_raw.csv', 'CONSISTENCY:monto_total',       NOW(), 17, 17, 0, 100.00, 'monto_total correcto en todos los registros limpios', 'batch_20260311');
```

---

## 6. Política de Retención de Datos en Staging

| Tabla | Retención | Justificación |
|---|---|---|
| `staging.ventas_raw` | 7 días | Datos crudos solo necesarios para re-procesar si hay falla |
| `staging.ventas_clean` | 30 días | Permite auditoría y comparación post-carga |
| `control.data_quality_log` | 1 año | Trazabilidad y cumplimiento regulatorio |
| `control.etl_execution_log` | 1 año | Auditoría de operaciones del pipeline |
