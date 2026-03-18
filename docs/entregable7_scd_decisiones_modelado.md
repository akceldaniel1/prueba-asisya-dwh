# Entregable 7 – Estrategia SCD y Decisiones de Modelado
**Proyecto:** Prueba Técnica Data Engineer – ASISYA  
**Fase:** 2 – Diseño del Data Warehouse  
**Fecha:** Marzo 2026  

---

## 1. Modelo Elegido: Esquema Estrella

Se eligió el **esquema estrella** (Star Schema) sobre el esquema copo de nieve (Snowflake) por las siguientes razones:

| Criterio | Estrella  | Copo de nieve |
|---|---|---|
| Rendimiento en queries | Mayor – menos JOINs | Menor – más JOINs por normalización |
| Simplicidad para BI | Alta – Power BI lo lee directamente | Media – requiere más configuración |
| Mantenimiento | Sencillo | Más complejo |
| Redundancia de datos | Leve | Mínima |
| Caso de uso | OLAP / reportes | OLTP / transaccional |

Para el volumen de datos de ASISYA (ventas diarias), el esquema estrella es la opción óptima. Si el volumen creciera 100x, se evaluaría snowflake o particionamiento.

---

## 2. Estrategia SCD por Dimensión

SCD (Slowly Changing Dimensions) define cómo manejar los cambios en los atributos de una dimensión a lo largo del tiempo.

### 2.1 Tipos de SCD disponibles

| Tipo | Comportamiento | Cuándo usarlo |
|---|---|---|
| **Type 0** | No se actualiza nada | Atributos que nunca cambian |
| **Type 1** | Se sobreescribe el valor anterior | Cuando el historial no importa |
| **Type 2** | Se crea un nuevo registro y se cierra el anterior | Cuando el historial sí importa |
| **Type 3** | Se agrega una columna "valor anterior" | Cuando solo importa el cambio inmediato anterior |

### 2.2 Decisión por cada dimensión

| Dimensión | Tipo SCD | Justificación |
|---|---|---|
| `dim_fecha` | **Type 0** | Las fechas no cambian jamás |
| `dim_cliente` | **Type 2** | Un cliente puede cambiar de ciudad, email o teléfono. Es importante saber con qué datos hizo cada compra histórica |
| `dim_producto` | **Type 1** | Los cambios de categoría o nombre se sobreescriben. El precio histórico de venta ya está en `fact_ventas` |
| `dim_vendedor` | **Type 1** | Cambios de región se sobreescriben. El historial de ventas está en `fact_ventas` |
| `dim_canal` | **Type 0** | Los canales son estables y no cambian |
| `dim_ciudad` | **Type 0** | Las ciudades y sus atributos son estables |

---

## 3. Implementación de SCD Type 2 en dim_cliente

### 3.1 Estructura de control

La tabla `dim_cliente` tiene tres columnas de control SCD:

```sql
fecha_vigencia_desde  DATE     -- desde cuándo es válido este registro
fecha_vigencia_hasta  DATE     -- hasta cuándo fue válido (NULL = vigente)
es_registro_actual    BOOLEAN  -- TRUE solo en el registro más reciente
```

### 3.2 Ejemplo visual

Supongamos que el cliente 2001 empieza en Bogotá y luego se muda a Medellín:

| cliente_key | id_cliente | ciudad | fecha_vigencia_desde | fecha_vigencia_hasta | es_registro_actual |
|---|---|---|---|---|---|
| 1 | 2001 | Bogotá | 2024-01-01 | 2024-06-15 | FALSE |
| 8 | 2001 | Medellín | 2024-06-16 | NULL | TRUE |

La venta de enero quedó asociada al `cliente_key = 1` (Bogotá).  
La venta de julio quedará asociada al `cliente_key = 8` (Medellín).  
Ambas son correctas históricamente.

### 3.3 Lógica del pipeline cuando detecta un cambio

```sql
-- PASO 1: Cerrar el registro anterior
UPDATE dwh.dim_cliente
SET
    fecha_vigencia_hasta = CURRENT_DATE - 1,
    es_registro_actual   = FALSE,
    fecha_actualizacion  = CURRENT_TIMESTAMP
WHERE id_cliente = :id_cliente
  AND es_registro_actual = TRUE;

-- PASO 2: Insertar el nuevo registro con los datos actualizados
INSERT INTO dwh.dim_cliente (
    id_cliente, email_cliente, telefono_cliente, ciudad,
    fecha_vigencia_desde, fecha_vigencia_hasta, es_registro_actual
)
VALUES (
    :id_cliente, :nuevo_email, :nuevo_telefono, :nueva_ciudad,
    CURRENT_DATE, NULL, TRUE
);
```

### 3.4 Lookup en fact_ventas

Cuando se carga un hecho, siempre se busca el registro **activo** del cliente:

```sql
SELECT cliente_key
FROM dwh.dim_cliente
WHERE id_cliente = :id_cliente
  AND es_registro_actual = TRUE;
```

Esto asegura que cada venta queda asociada a la versión vigente del cliente en el momento de la carga.

---

## 4. Diseño de fact_ventas

### 4.1 Medidas incluidas

| Medida | Tipo | Descripción |
|---|---|---|
| `cantidad` | Aditiva | Se puede sumar en cualquier dimensión |
| `precio_unitario` | Semi-aditiva | No se suma entre clientes, sí en el tiempo |
| `monto_total` | Aditiva | Medida principal de análisis |
| `descuento` | Aditiva | Descuento aplicado a la transacción |
| `impuestos` | Aditiva | Impuestos de la transacción |
| `monto_neto` | Aditiva (generada) | `monto_total - descuento + impuestos` |

### 4.2 Idempotencia

La constraint `UNIQUE (id_venta)` garantiza que si el DAG se re-ejecuta, no se duplican registros. El pipeline usa `INSERT ... ON CONFLICT DO NOTHING` para manejar esto.

### 4.3 `fecha_venta` desnormalizado

Se incluye `fecha_venta DATE` directamente en `fact_ventas` además del `fecha_key`. Esto permite queries de rango de fechas sin hacer JOIN a `dim_fecha`, mejorando la performance en filtros temporales frecuentes.

---

## 5. Decisiones de Índices

### 5.1 Criterio de selección

Se crearon índices basados en los patrones de consulta más frecuentes en reportes de ventas:

| Índice | Tipo | Justifica |
|---|---|---|
| `idx_fv_fecha_key` | Simple | Filtros por período (mes, año, trimestre) |
| `idx_fv_cliente_key` | Simple | Análisis por cliente |
| `idx_fv_producto_key` | Simple | Top productos, análisis de portafolio |
| `idx_fv_vendedor_key` | Simple | Rankings de vendedores |
| `idx_fv_fecha_producto` | **Compuesto** | Tendencias de producto por período (YoY) |
| `idx_fv_vendedor_fecha` | **Compuesto** | Performance de vendedor por período |
| `idx_fv_ciudad_canal` | **Compuesto** | Análisis geográfico por canal |
| `idx_fv_fecha_venta DESC` | Simple | Queries de "últimas N ventas" |

### 5.2 Índice parcial en dim_cliente

```sql
CREATE UNIQUE INDEX uq_dim_cliente_activo
ON dwh.dim_cliente (id_cliente)
WHERE es_registro_actual = TRUE;
```

Este índice parcial garantiza a nivel de base de datos que nunca pueda existir más de un registro activo por cliente, y además acelera el lookup del registro vigente.

---

## 6. Particionamiento (consideración)

Para el volumen actual (decenas de miles de registros anuales), el particionamiento **no es necesario**. Se recomienda implementarlo cuando `fact_ventas` supere los **10 millones de registros**.

Estrategia propuesta para escalar:

```sql
-- Particionamiento por rango de año
CREATE TABLE dwh.fact_ventas (...)
PARTITION BY RANGE (fecha_venta);

CREATE TABLE dwh.fact_ventas_2024
    PARTITION OF dwh.fact_ventas
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

CREATE TABLE dwh.fact_ventas_2025
    PARTITION OF dwh.fact_ventas
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
```

Beneficio: queries con filtro por año solo escanean la partición correspondiente, reduciendo el tiempo de respuesta significativamente.

---

## 7. Trade-offs Considerados

| Decisión | Alternativa descartada | Por qué se eligió la actual |
|---|---|---|
| Estrella vs Snowflake | Snowflake | Mejor rendimiento en Power BI con menos JOINs |
| SCD2 solo en cliente | SCD2 en producto también | El precio histórico ya está en fact_ventas; normalizar producto agrega complejidad sin beneficio |
| fecha_key como INTEGER | DATE directamente | Permite joins más rápidos y es práctica estándar en DWH |
| monto_neto GENERATED | Calculada en app | La BD garantiza siempre la consistencia del valor |
| UNIQUE en id_venta | Lógica solo en ETL | La constraint a nivel BD es la última línea de defensa ante duplicados |
