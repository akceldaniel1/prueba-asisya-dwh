# Prueba Técnica – Data Engineer ASISYA
**Candidato:** [Tu nombre]  
**Fecha de entrega:** Marzo 2026  
**Stack:** PostgreSQL 15 · Apache Airflow 2.x · Python 3.9+ · Power BI · Docker

---

## Resumen de la solución

Solución completa de ingeniería de datos que cubre gobernanza de datos, diseño de Data Warehouse dimensional, pipeline ETL orquestado con Airflow, análisis SQL avanzado, dashboard ejecutivo en Power BI y controles de seguridad y compliance alineados con la Ley 1581 y GDPR.

**Highlights:**
- Pipeline ETL idempotente con 3 niveles de protección contra duplicados
- SCD Type 2 implementado en `dim_cliente` para tracking histórico
- RLS configurado tanto en PostgreSQL como en Power BI (2 roles)
- Score de calidad de datos: **99.2%** sobre el dataset de prueba
- Festivos colombianos 2020–2030 implementados con lógica de traslado al lunes
- Procedimiento de derecho al olvido (Ley 1581 / GDPR Art. 17)

---

## Estructura del Repositorio

```
asisya-dwh/
├── README.md                          ← Este archivo
├── requirements.txt                   ← Dependencias Python
│
├── airflow/
│   ├── dags/
│   │   └── entregable9_etl_ventas_asisya.py   ← DAG principal (Fase 3)
│   └── entregable10_README_setup.md           ← Instrucciones de configuración Airflow
│
├── sql/
│   ├── ddl/
│   │   └── entregable5_ddl_completo.sql       ← Esquema completo del DWH (Fase 2)
│   ├── dml/
│   │   └── entregable8_populate_dimensiones.sql ← Seed de dimensiones estáticas (Fase 2)
│   ├── queries/
│   │   ├── entregable11_scripts_adicionales.sql ← Scripts de apoyo ETL (Fase 3)
│   │   └── entregable14_queries_analiticos.sql  ← 7 queries SQL avanzados (Fase 4)
│   └── security/
│       ├── entregable18_rbac_rls.sql           ← RBAC + RLS en PostgreSQL (Fase 6)
│       └── entregable20_anonimizacion.sql       ← Funciones de anonimización (Fase 6)
│
├── scripts/
│   └── data_profiling.py                       ← Script de validación de datos (Fase 1)
│
├── powerbi/
│   ├── dashboard_asisya.pbix                   ← Dashboard ejecutivo (Fase 5)
│   └── fase5_medidas_dax.dax                   ← Medidas DAX documentadas (Fase 5)
│
└── docs/
    ├── entregable1_reporte_profiling.md         ← Reporte de data profiling (Fase 1)
    ├── entregable2_diccionario_datos.md         ← Diccionario de datos (Fase 1)
    ├── entregable3_reglas_calidad_gobierno.md   ← Reglas de calidad (Fase 1)
    ├── entregable6_erd_dbdiagram.dbml           ← Código ERD para dbdiagram.io (Fase 2)
    ├── entregable7_scd_decisiones_modelado.md   ← Estrategia SCD y decisiones (Fase 2)
    ├── entregables12_13_evidencia_incremental.md ← Evidencia ETL + estrategia incremental (Fase 3)
    ├── fase4_sql_avanzado_completo.docx         ← Documento SQL avanzado (Fase 4)
    ├── fase5_powerbi_rls_completo.docx          ← Documento Power BI + RLS (Fase 5)
    └── fase6_seguridad_compliance.docx          ← Documento seguridad y compliance (Fase 6)
```

---

## Setup Rápido

### 1. Prerequisitos

| Herramienta | Versión |
|---|---|
| Docker Desktop | 4.x+ |
| Docker Compose | 2.x+ |
| Python | 3.9+ |
| PostgreSQL | 14+ |

### 2. Instalar dependencias Python

```bash
pip install -r requirements.txt
```

### 3. Preparar la base de datos

```bash
# Opción A: PostgreSQL local (pgAdmin)
# 1. Crear base de datos: asisya
# 2. Ejecutar en orden:
psql -U postgres -d asisya -f sql/ddl/entregable5_ddl_completo.sql
psql -U postgres -d asisya -f sql/dml/entregable8_populate_dimensiones.sql
```

### 4. Configurar y ejecutar Airflow

Ver instrucciones detalladas en [`airflow/entregable10_README_setup.md`](airflow/entregable10_README_setup.md)

```bash
# Setup con Docker Compose
docker-compose up -d

# Configurar conexión en Airflow UI (http://localhost:8080)
# Connection Id: postgres_dwh
# Host: localhost | Database: asisya | Login: postgres
```

### 5. Ejecutar el pipeline

```bash
# Carga inicial
airflow dags trigger etl_ventas_asisya --conf '{"mode": "initial"}'

# Carga incremental
airflow dags trigger etl_ventas_asisya --conf '{"mode": "incremental"}'
```

### 6. Verificar resultados

```sql
-- Registros cargados
SELECT COUNT(*), SUM(monto_total) FROM dwh.fact_ventas;
-- Esperado: 24 registros | $1.801.000

-- Score de calidad
SELECT regla_calidad, porcentaje_calidad 
FROM control.data_quality_log 
ORDER BY fecha_validacion DESC LIMIT 10;
```

---

## Fases implementadas

| Fase | Descripción | Peso | Estado |
|---|---|---|---|
| 1 | Gobernanza y Calidad de Datos | 15% | ✅ Completa |
| 2 | Diseño del Data Warehouse | 15% | ✅ Completa |
| 3 | Pipeline ETL con Airflow | 25% | ✅ Completa |
| 4 | SQL Avanzado y Análisis | 15% | ✅ Completa |
| 5 | Dashboard Power BI | 15% | ✅ Completa |
| 6 | Seguridad y Compliance | 10% | ✅ Completa |
| 7 | Arquitectura Escalable | 10% | ⏳ No incluida |

---

## Decisiones técnicas clave

- **Esquema estrella** sobre snowflake: mejor rendimiento en Power BI y queries analíticos con menos JOINs
- **SCD Type 2 solo en dim_cliente**: el precio histórico ya está en `fact_ventas`; normalizar producto agrega complejidad sin beneficio
- **Idempotencia en 3 niveles**: batch_id en staging, UNIQUE constraint en `fact_ventas`, y detección previa en Python
- **Rechazo de precio_unitario nulo**: imputar con la media distorsionaría KPIs de ventas y comisiones
- **FORCE ROW LEVEL SECURITY**: protege contra accesos accidentales incluso por el dueño de la tabla
- **Derecho al olvido por anonimización**: no se eliminan registros para preservar integridad referencial; se enmascaran los datos personales

---

## Contacto

[Tu nombre]  
[Tu email]  
[Tu LinkedIn]
