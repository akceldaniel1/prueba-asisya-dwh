# Entregable 10 – README: Configuración y Ejecución del Pipeline ETL

**Proyecto:** Prueba Técnica Data Engineer – ASISYA  
**Fase:** 3 – Pipeline ETL con Apache Airflow  

---

## 1. Requisitos Previos

| Herramienta | Versión mínima |
|---|---|
| Docker Desktop | 4.x |
| Docker Compose | 2.x |
| Python | 3.9+ |
| PostgreSQL | 14+ |
| Apache Airflow | 2.x |

---

## 2. Estructura de Archivos

```
proyecto-asisya/
├── airflow/
│   ├── dags/
│   │   └── entregable9_etl_ventas_asisya.py   ← DAG principal
│   └── docker-compose.yml                      ← Setup local
├── sql/
│   ├── ddl/
│   │   └── entregable5_ddl_completo.sql        ← Crear esquema
│   └── dml/
│       └── entregable8_populate_dimensiones.sql ← Poblar dims estáticas
└── data/
    ├── ventas_raw.csv
    └── ventas_nuevas.csv
```

---

## 3. Setup con Docker Compose (recomendado)

### Paso 1 – Crear docker-compose.yml

```yaml
version: '3.8'
services:

  postgres:
    image: postgres:15
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./sql:/docker-entrypoint-initdb.d

  airflow-init:
    image: apache/airflow:2.8.0
    depends_on: [postgres]
    environment:
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
    command: db init

  airflow-webserver:
    image: apache/airflow:2.8.0
    depends_on: [airflow-init]
    environment:
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
    ports:
      - "8080:8080"
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./data:/opt/airflow/data
    command: webserver

  airflow-scheduler:
    image: apache/airflow:2.8.0
    depends_on: [airflow-init]
    environment:
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./data:/opt/airflow/data
    command: scheduler

volumes:
  postgres_data:
```

### Paso 2 – Levantar los servicios

```bash
docker-compose up -d
```

### Paso 3 – Crear el esquema del DWH

```bash
# Conectarse al contenedor de PostgreSQL
docker exec -it <postgres_container_id> psql -U airflow -d airflow

# Ejecutar el DDL
\i /docker-entrypoint-initdb.d/ddl/entregable5_ddl_completo.sql

# Poblar dimensiones estáticas
\i /docker-entrypoint-initdb.d/dml/entregable8_populate_dimensiones.sql
```

---

## 4. Configuración en Airflow UI

Abrir en el navegador: **http://localhost:8080**  
Usuario: `admin` | Contraseña: `admin`

### 4.1 Crear Conexión a PostgreSQL

```
Admin → Connections → Add Connection

Connection Id:   postgres_dwh
Connection Type: Postgres
Host:            postgres
Port:            5432
Database:        airflow
Login:           airflow
Password:        airflow
```

### 4.2 Crear Variables

```
Admin → Variables → Add

csv_raw_path       /opt/airflow/data/ventas_raw.csv
csv_new_path       /opt/airflow/data/ventas_nuevas.csv
quality_threshold  0.75
retention_days     7
```

---

## 5. Ejecución del Pipeline

### 5.1 Carga Inicial

```
DAGs → etl_ventas_asisya → Trigger DAG w/ config

Config JSON:
{
  "mode": "initial"
}
```

### 5.2 Carga Incremental

```
DAGs → etl_ventas_asisya → Trigger DAG w/ config

Config JSON:
{
  "mode": "incremental"
}
```

---

## 6. Verificación de Resultados

Después de la ejecución, verificar en PostgreSQL:

```sql
-- Registros en fact_ventas
SELECT COUNT(*) FROM dwh.fact_ventas;

-- Score de calidad del último batch
SELECT regla_calidad, porcentaje_calidad, detalles
FROM control.data_quality_log
ORDER BY fecha_validacion DESC LIMIT 10;

-- Log de ejecución
SELECT proceso, estado, registros_exitosos, registros_fallidos, fecha_fin
FROM control.etl_execution_log
ORDER BY fecha_inicio DESC LIMIT 10;

-- Vista completa de ventas cargadas
SELECT * FROM dwh.v_ventas_completas LIMIT 5;
```

---

## 7. Solución de Problemas Frecuentes

| Error | Causa probable | Solución |
|---|---|---|
| `Connection refused postgres_dwh` | Conexión no creada en Airflow UI | Revisar paso 4.1 |
| `fecha_key not found` | dim_fecha no poblada | Ejecutar entregable8_populate_dimensiones.sql |
| `Quality score below threshold` | Datos con muchos errores | Revisar control.data_quality_log |
| `UNIQUE violation id_venta` | Re-ejecución con datos ya cargados | Normal – ON CONFLICT DO NOTHING lo maneja |
| DAG no aparece en UI | Error de sintaxis en el .py | Revisar logs del scheduler |
