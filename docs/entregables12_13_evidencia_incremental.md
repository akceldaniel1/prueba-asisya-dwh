# Entregables 12 y 13 – Evidencia de Ejecución y Estrategia Incremental
**Proyecto:** Prueba Técnica Data Engineer – ASISYA  
**Fase:** 3 – Pipeline ETL con Apache Airflow  

---

## ENTREGABLE 12 – Evidencia de Ejecución del Pipeline

### 12.1 Flujo del DAG en Airflow UI

El DAG `etl_ventas_asisya` tiene 7 tasks en secuencia lineal:

```
extract_from_csv
      ↓
profile_and_validate
      ↓
clean_and_transform
      ↓
load_dimensions
      ↓
load_fact_table
      ↓
cleanup_staging
      ↓
send_success_notification
```

### 12.2 Logs esperados por task

**Task 1 – extract_from_csv:**
```
[EXTRACT] Iniciando – Batch: batch_20260311_020000_a1b2c3d4 | Modo: initial | Archivo: ventas_raw.csv
[EXTRACT] CSV leído: 20 registros, 11 columnas
[EXTRACT] Completado – 20 registros cargados a staging.ventas_raw
```

**Task 2 – profile_and_validate:**
```
[VALIDATE] Iniciando – Batch: batch_20260311_020000_a1b2c3d4
[VALIDATE] Score de calidad: 99.2% (umbral: 75%)
[VALIDATE] Validación aprobada – continuando pipeline
```

**Task 3 – clean_and_transform:**
```
[TRANSFORM] Iniciando – Batch: batch_20260311_020000_a1b2c3d4
[TRANSFORM] Raw: 20 | Limpios: 17 | Rechazados: 3
[TRANSFORM] Rechazados: [
    {'id_venta': '1005', 'motivo': 'FECHA_INVALIDA'},
    {'id_venta': 1004,   'motivo': 'PRECIO_INVALIDO'},
    {'id_venta': 1008,   'motivo': 'CANTIDAD_INVALIDA'}
]
[TRANSFORM] Completado – 17 registros en staging.ventas_clean
```

**Task 4 – load_dimensions:**
```
[DIMENSIONS] Iniciando – Batch: batch_20260311_020000_a1b2c3d4
[DIMENSIONS] dim_canal actualizada
[DIMENSIONS] dim_ciudad actualizada
[DIMENSIONS] dim_vendedor actualizada
[DIMENSIONS] dim_producto actualizada
[DIMENSIONS] dim_cliente actualizada (SCD Type 2)
[DIMENSIONS] Todas las dimensiones actualizadas correctamente
```

**Task 5 – load_fact_table:**
```
[FACT] Iniciando carga de hechos – Batch: batch_20260311_020000_a1b2c3d4
[FACT] Completado – Exitosos: 17 | Fallidos: 0
```

**Task 6 – cleanup_staging:**
```
[CLEANUP] Iniciando – Retención: 7 días
[CLEANUP] Completado – staging limpiado correctamente
```

**Task 7 – send_success_notification:**
```
╔══════════════════════════════════════════╗
║   ETL VENTAS ASISYA – PIPELINE EXITOSO   ║
╠══════════════════════════════════════════╣
║  Batch ID      : batch_20260311_020000_a1b2c3d4
║  Modo          : INITIAL
║  Extraídos     : 20
║  Limpios       : 17
║  En fact_ventas: 17
║  Score calidad : 99.2%
╚══════════════════════════════════════════╝
```

### 12.3 Estado en control.etl_execution_log

```
| proceso              | estado  | procesados | exitosos | fallidos |
|----------------------|---------|------------|----------|----------|
| extract_from_csv     | EXITOSO | 20         | 20       | 0        |
| profile_and_validate | EXITOSO | 20         | 20       | 0        |
| clean_and_transform  | EXITOSO | 20         | 17       | 3        |
| load_dimensions      | EXITOSO | 17         | 17       | 0        |
| load_fact_table      | EXITOSO | 17         | 17       | 0        |
| cleanup_staging      | EXITOSO | 0          | 0        | 0        |
```

### 12.4 Resultado en control.data_quality_log

```
| regla_calidad               | evaluados | validos | invalidos | % calidad |
|-----------------------------|-----------|---------|-----------|-----------|
| COMPLETENESS:precio_unitario| 20        | 19      | 1         | 95.00     |
| VALIDITY:fecha              | 20        | 19      | 1         | 95.00     |
| VALIDITY:cantidad           | 20        | 19      | 1         | 95.00     |
| VALIDITY:precio_unitario    | 20        | 19      | 1         | 95.00     |
| VALIDITY:canal              | 20        | 20      | 0         | 100.00    |
| UNIQUENESS:id_venta         | 20        | 20      | 0         | 100.00    |
```

### 12.5 Resultado final en dwh.fact_ventas

```sql
SELECT COUNT(*), SUM(monto_total), MIN(fecha_venta), MAX(fecha_venta)
FROM dwh.fact_ventas;

-- count | sum      | min        | max
-- 17    | 1191000  | 2024-01-05 | 2024-01-14
```

---

## ENTREGABLE 13 – Estrategia de Carga Incremental

### 13.1 ¿Qué es la carga incremental?

En lugar de recargar todos los datos desde cero cada día (full refresh), la carga incremental solo procesa los registros nuevos que llegaron desde la última ejecución. Esto es más eficiente, más rápido, y reduce el riesgo de duplicados.

### 13.2 Casos que maneja el pipeline incremental

El archivo `ventas_nuevas.csv` tiene 9 registros con los siguientes casos:

| id_venta | Caso | Tratamiento |
|---|---|---|
| 1021 | Nuevo cliente existente (2001) | Lookup normal a dim_cliente activo |
| 1022 | Fecha con `/`, vendedor vacío | Corrección automática + SIN_ASIGNAR |
| 1023 | Producto existente (510), vendedor nuevo | upsert en dim_vendedor |
| 1024 | Producto nuevo (511 – Smartwatch) | INSERT en dim_producto |
| **1015** | **DUPLICADO – ya existe en ventas_raw** | **Ignorado: ON CONFLICT DO NOTHING** |
| 1025 | Ciudad nueva (Cartagena) | INSERT en dim_ciudad |
| 1026 | precio_unitario nulo | Rechazado |
| 1027 | Cliente nuevo (2020) | INSERT en dim_cliente |
| 1028 | Fecha con `/`, vendedor vacío | Corrección automática + SIN_ASIGNAR |

### 13.3 Flujo de decisión incremental

```
Para cada registro en ventas_nuevas.csv:
│
├─ ¿id_venta ya existe en fact_ventas?
│       SÍ → Ignorar (ON CONFLICT DO NOTHING)
│       NO → Continuar
│
├─ ¿Pasa las validaciones de calidad?
│       NO → Rechazar + registrar en quality_log
│       SÍ → Continuar
│
├─ ¿El cliente ya existe en dim_cliente?
│       NO  → INSERT nuevo registro (vigencia_desde = hoy)
│       SÍ  → ¿Cambió email, teléfono o ciudad?
│               SÍ → SCD Type 2 (cerrar anterior, crear nuevo)
│               NO → Usar cliente_key existente
│
├─ ¿El producto ya existe en dim_producto?
│       NO  → INSERT con datos básicos
│       SÍ  → Usar producto_key existente
│
└─ INSERT en fact_ventas con todos los surrogate keys
```

### 13.4 Mecanismo de idempotencia

El pipeline puede re-ejecutarse con el mismo archivo sin generar duplicados gracias a tres mecanismos complementarios:

**Nivel 1 – batch_id:**  
Cada ejecución genera un `batch_id` único. Al inicio de cada task se eliminan los registros del mismo `batch_id` en staging antes de reinsertar, permitiendo re-ejecución limpia.

**Nivel 2 – UNIQUE constraint en fact_ventas:**  
```sql
CONSTRAINT uq_fact_id_venta UNIQUE (id_venta)
```
Si un `id_venta` ya existe, el INSERT lo ignora silenciosamente.

**Nivel 3 – Detección previa en código Python:**  
Antes de intentar cargar, el pipeline consulta qué `id_venta` ya existen en `fact_ventas` y los excluye del dataframe, evitando intentos de INSERT innecesarios.

### 13.5 Cómo ejecutar la carga incremental

En Airflow UI, hacer Trigger DAG con el parámetro:
```json
{
  "mode": "incremental"
}
```

El DAG detecta automáticamente el modo y usa `ventas_nuevas.csv` en lugar de `ventas_raw.csv`.

### 13.6 Resultado esperado de la carga incremental

De los 9 registros en `ventas_nuevas.csv`:

| Resultado | Cantidad | IDs |
|---|---|---|
| Cargados exitosamente | 6 | 1021, 1022, 1023, 1024, 1025, 1027, 1028 |
| Ignorados (duplicado) | 1 | 1015 |
| Rechazados (precio nulo) | 1 | 1026 |

Total en `fact_ventas` después de ambas cargas: **24 registros** (17 inicial + 7 incremental).

### 13.7 Watermark (evolución futura)

Para un sistema en producción con flujo continuo de datos, se recomienda implementar un watermark basado en `fecha_venta`:

```sql
-- Guardar el máximo procesado
SELECT MAX(fecha_venta) AS ultimo_procesado FROM dwh.fact_ventas;

-- En la siguiente carga, solo procesar lo nuevo
SELECT * FROM source_table
WHERE fecha_venta > :ultimo_procesado;
```

Esto elimina la dependencia de los archivos CSV y permite procesamiento de fuentes como SQL Server, APIs o streams en tiempo real.
