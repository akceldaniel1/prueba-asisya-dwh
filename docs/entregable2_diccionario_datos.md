# Entregable 2 – Diccionario de Datos
**Proyecto:** Prueba Técnica Data Engineer – ASISYA  
**Fuente:** `ventas_raw.csv` / `ventas_nuevas.csv`  
**Destino:** `staging.ventas_raw` → `staging.ventas_clean` → `dwh.fact_ventas`  
**Fecha:** Marzo 2026  

---

## 1. Tabla: `ventas_raw.csv` (fuente)

| # | Campo | Tipo en CSV | Tipo en DWH | Descripción | ¿Requerido? | Regla de Negocio | Ejemplo Válido | Ejemplo Inválido |
|---|---|---|---|---|---|---|---|---|
| 1 | `id_venta` | VARCHAR | INTEGER | Identificador único de cada transacción de venta |  Sí | Entero positivo. Único por archivo y entre cargas. | `1001` | `abc`, duplicado |
| 2 | `fecha` | VARCHAR | DATE | Fecha en que se realizó la venta |  Sí | Formato `YYYY-MM-DD` o `YYYY/MM/DD`. Mes entre 1-12. Día válido según mes. Rango: 2020–2030. | `2024-01-05` | `2024-13-07`, `2024/99/01` |
| 3 | `id_cliente` | VARCHAR | INTEGER | Identificador del cliente que realizó la compra |  Sí | Entero positivo. Referencia a `dim_cliente`. Si es nuevo se crea automáticamente. | `2001` | `0`, negativo |
| 4 | `id_producto` | VARCHAR | INTEGER | Identificador del producto vendido |  Sí | Entero positivo. Referencia a `dim_producto`. Si es nuevo se crea automáticamente. | `501` | `0`, texto |
| 5 | `cantidad` | VARCHAR | INTEGER | Número de unidades vendidas en la transacción |  Sí | Entero ≥ 1. Valores negativos o cero indican error o devolución (proceso separado). | `2` | `-2`, `0`, `1.5` |
| 6 | `precio_unitario` | VARCHAR | NUMERIC(12,2) | Precio por unidad al momento de la venta (en COP) |  Sí | Decimal > 0. No se imputa si está vacío: el registro se rechaza. | `35000` | vacío, `0`, `-100` |
| 7 | `canal` | VARCHAR | VARCHAR(50) | Canal de venta utilizado para la transacción |  Sí | Valores permitidos (insensible a mayúsculas): `Online`, `Tienda`, `Marketplace`. Se normaliza en transformación. | `Online` | `web`, `presencial` |
| 8 | `ciudad` | VARCHAR | VARCHAR(100) | Ciudad donde ocurrió la venta |  Sí | Texto no vacío. Referencia a `dim_ciudad`. Si es nueva se crea automáticamente. | `Bogotá` | vacío |
| 9 | `vendedor` | VARCHAR | VARCHAR(200) | Nombre completo del vendedor que realizó la venta |  No | Si está vacío se asigna `SIN_ASIGNAR`. No se usa como identificador único. Referencia a `dim_vendedor`. | `Juan Pérez` | *(se acepta vacío)* |
| 10 | `email_cliente` | VARCHAR | VARCHAR(200) | Correo electrónico del cliente |  Sí | Formato `usuario@dominio.ext`. No se usa como identificador único del cliente (puede cambiar). | `juan@email.com` | `juanemail.com`, vacío |
| 11 | `telefono_cliente` | VARCHAR | VARCHAR(50) | Número de teléfono del cliente |  Sí | 10 dígitos numéricos (formato Colombia). | `3001234567` | `300-123`, `123` |

---

## 2. Campos Calculados (generados en transformación)

| Campo | Tabla destino | Tipo | Fórmula | Descripción |
|---|---|---|---|---|
| `monto_total` | `staging.ventas_clean` | NUMERIC(15,2) | `cantidad × precio_unitario` | Valor bruto total de la transacción |
| `monto_neto` | `dwh.fact_ventas` | NUMERIC(15,2) | `monto_total - descuento + impuestos` | Valor neto final (columna generada en PostgreSQL) |
| `fecha_procesamiento` | `staging.ventas_clean` | TIMESTAMP | `CURRENT_TIMESTAMP` | Momento en que el registro fue procesado por el pipeline |
| `id_batch` | `staging.ventas_clean` | VARCHAR(100) | generado por DAG | Identificador del lote de procesamiento para trazabilidad |

---

## 3. Campos de Control (auditoría)

| Campo | Tabla | Tipo | Descripción |
|---|---|---|---|
| `fecha_carga` | `staging.ventas_raw` | TIMESTAMP | Momento de ingesta del archivo crudo |
| `archivo_origen` | `staging.ventas_raw` | VARCHAR(200) | Nombre del archivo CSV de origen |
| `id_batch` | Todas | VARCHAR(100) | Correlaciona registros con su ejecución de DAG |

---

## 4. Mapeo de Valores Permitidos

### Campo `canal`

| Valor en CSV (posibles) | Valor normalizado | Tipo de canal |
|---|---|---|
| `Online`, `online`, `ONLINE` | `Online` | Digital |
| `Tienda`, `tienda`, `TIENDA` | `Tienda` | Físico |
| `Marketplace`, `marketplace` | `Marketplace` | Digital |
| Cualquier otro valor | → Rechazar registro | – |

---

## 5. Linaje de Datos (Data Lineage)

```
ventas_raw.csv
    └─► staging.ventas_raw       (carga directa, sin transformar)
            └─► staging.ventas_clean   (limpieza + transformación)
                    └─► dwh.fact_ventas         (carga dimensional)
                    └─► dwh.dim_cliente         (SCD Type 2)
                    └─► dwh.dim_producto        (upsert)
                    └─► dwh.dim_vendedor        (upsert)
                    └─► dwh.dim_ciudad          (upsert)
```

---

## 6. Datos Sensibles (Compliance Ley 1581 / GDPR)

| Campo | Clasificación | Tratamiento recomendado |
|---|---|---|
| `email_cliente` | Dato personal | Cifrar en reposo. Enmascarar en entornos no-productivos. |
| `telefono_cliente` | Dato personal | Cifrar en reposo. Enmascarar en entornos no-productivos. |
| `id_cliente` | Identificador | Pseudoanonimizar si se comparte fuera del DWH. |
| `vendedor` |  Dato laboral | Acceso restringido por rol. |
