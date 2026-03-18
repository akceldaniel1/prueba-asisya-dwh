# Entregable 1 – Reporte de Data Profiling
**Proyecto:** Prueba Técnica Data Engineer – ASISYA  
**Dataset:** `ventas_raw.csv`   
**Registros analizados:** 20  
**Columnas:** 11  

---

## 1. Estadísticas Generales

| Indicador | Valor |
|---|---|
| Total de registros | 20 |
| Total de columnas | 11 |
| Duplicados por `id_venta` (interno) | 0 |
| Filas completamente duplicadas | 0 |
| Registros con al menos un problema | 6 |
| Registros limpios aptos para DWH | 17 |
| Registros rechazados (errores críticos) | 3 |

---

## 2. Análisis de Nulos por Columna

| Campo | Nulos | % Nulos | ¿Requerido? | Impacto |
|---|---|---|---|---|
| id_venta | 0 | 0% | Sí | – |
| fecha | 0 | 0% | Sí | – |
| id_cliente | 0 | 0% | Sí | – |
| id_producto | 0 | 0% | Sí | – |
| cantidad | 0 | 0% | Sí | – |
| **precio_unitario** | **1** | **5%** | Sí | Crítico |
| canal | 0 | 0% | Sí | – |
| ciudad | 0 | 0% | Sí | – |
| **vendedor** | **2** | **10%** | No | Manejable |
| email_cliente | 0 | 0% | Sí | – |
| telefono_cliente | 0 | 0% | Sí | – |

---

## 3. Estadísticas Descriptivas por Campo Numérico

| Campo | Mín | Máx | Media | Mediana | Valores únicos |
|---|---|---|---|---|---|
| id_venta | 1001 | 1020 | 1010.5 | 1010.5 | 20 |
| id_cliente | 2001 | 2015 | 2007.5 | 2007 | 15 |
| id_producto | 501 | 510 | 504.3 | 503 | 10 |
| cantidad | **-2** | 5 | 2.1 | 2 | 6 |
| precio_unitario | 12000 | 80000 | 34684 | 35000 | 9 |

>  El mínimo de `cantidad` es -2, lo que indica un valor inválido en el registro 1008.

---

## 4. Distribución de Variables Categóricas

### Canal de venta (valores raw, sin normalizar)

| Valor raw | Frecuencia | Estado |
|---|---|---|
| Online | 7 | Correcto |
| Tienda | 6 | Correcto |
| online | 3 | Capitalización incorrecta |
| Marketplace | 2 | Correcto |
| marketplace | 2 | Capitalización incorrecta |

### Ciudad

| Ciudad | Frecuencia |
|---|---|
| Bogotá | 8 |
| Cali | 5 |
| Medellín | 5 |
| Barranquilla | 2 |

### Vendedor

| Vendedor | Frecuencia |
|---|---|
| Juan Pérez | 6 |
| Laura Díaz | 5 |
| Camilo Rojas | 3 |
| Ana Torres | 2 |
| Roberto Vega | 2 |
| *(vacío)* | 2 |

---

## 5. Problemas Detectados

###  Errores Críticos – Registro Rechazado

| ID Venta | Campo | Valor problemático | Tipo de error |
|---|---|---|---|
| 1005 | fecha | `2024-13-07` | Fecha imposible (mes 13) |
| 1004 | precio_unitario | *(vacío)* | Campo requerido nulo |
| 1008 | cantidad | `-2` | Valor negativo inválido |

###  Inconsistencias – Corregibles en transformación

| ID Venta | Campo | Valor raw | Corrección aplicada |
|---|---|---|---|
| 1003 | fecha | `2024/01/06` | → `2024-01-06` |
| 1003 | canal | `online` | → `Online` |
| 1003 | vendedor | *(vacío)* | → `SIN_ASIGNAR` |
| 1009 | canal | `marketplace` | → `Marketplace` |
| 1012 | vendedor | *(vacío)* | → `SIN_ASIGNAR` |
| 1015 | canal | `online` | → `Online` |

### 🔵 Alerta entre archivos

| ID Venta | Detalle |
|---|---|
| 1015 | Presente en `ventas_raw.csv` Y `ventas_nuevas.csv` con datos idénticos. En carga incremental debe ignorarse para evitar duplicación. |

---

## 6. Detección de Outliers (método IQR)

**Campo: `cantidad`**
- Q1 = 1, Q3 = 3, IQR = 2
- Rango esperado: [-2, 6]
- Outliers detectados: ninguno fuera del rango (el -2 ya fue rechazado por regla de negocio)

**Campo: `precio_unitario`**
- Q1 = 15000, Q3 = 45000, IQR = 30000
- Rango esperado: [-30000, 90000]
- Outliers detectados: ninguno estadístico, aunque `80000` (teclado mecánico) es el valor más alto y está dentro del rango aceptable

---

## 7. Score de Calidad Global

| Dimensión | Regla evaluada | Registros válidos | % |
|---|---|---|---|
| Completeness | precio_unitario no nulo | 19/20 | 95% |
| Validity | fecha parseable y válida | 19/20 | 95% |
| Validity | cantidad ≥ 1 | 19/20 | 95% |
| Validity | canal en lista permitida | 20/20 | 100% |
| Uniqueness | id_venta sin duplicados internos | 20/20 | 100% |
| Uniqueness | id_venta sin duplicados entre archivos | 19/20 | 95% |


---