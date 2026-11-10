# Proyecto 6: Seguimiento Epidemiológico de COVID-19 / OWID

Plataforma integral de ingeniería de datos para la extracción, procesamiento distribuido y persistencia analítica de datos epidemiológicos globales basados en **Our World in Data (OWID)**. Diseñada bajo una arquitectura moderna con **FastAPI**, **Apache PySpark 3.5**, **MySQL (Aiven Cloud / Local)** y consumo analítico en **Power BI** vía ODBC.

---

## 1. Arquitectura del Sistema

```
[ Fuente Externa: OWID CSV ]
             │
             ▼ (Descarga HTTP en bloques / Stream)
[ Contenedor Docker: FastAPI + PySpark Engine ]
   ├── /api/extract    --> Ingesta y validación de esquema en SparkSession
   ├── /api/transform  --> Deduplicación, imputación, ventana móvil 7d y KPIs
   └── /api/load       --> Inserción masiva JDBC (batchsize 10.000 + rewriteBatchedStatements)
             │
      ┌──────┴──────────────────────────┐
      ▼ (Modo Nube)                     ▼ (Modo Local)
[ MySQL en Aiven Cloud ]     [ MySQL Local / Docker ]
   (SSL Obligatorio TLS)        (Entorno de desarrollo local)
      │
      ▼ (ODBC Unicode 64-bit)
[ Power BI Desktop ]
   (Dashboards ejecutivos e inteligencia epidemiológica)
```

---

## 2. Pila Tecnológica

* **Backend & API:** FastAPI, Uvicorn, Pydantic, Jinja2 (Dashboard web interactivo).
* **Motor ETL Distribuido:** Apache PySpark 3.5 (sobre Java 17 OpenJDK Headless).
* **Persistencia Relacional:** MySQL 8.4 (Aiven Cloud con conexión TLS segura o MySQL en Docker).
* **Conector JDBC:** MySQL Connector/J (`com.mysql:mysql-connector-j:8.4.0`) optimizado con sentencias por lotes reescritas.
* **Consumo de BI:** Power BI Desktop mediante ODBC Unicode Driver de 64 bits.
* **Contenerización y Despliegue:** Docker, Docker Compose, Railway.

---

## 3. Estructura del Repositorio

```text
COVID-19/
├── static/
│   └── css/style.css        # Hoja de estilos del dashboard web interactivo
├── templates/
│   └── index.html           # Interfaz web para control del pipeline (4 fases)
├── .dockerignore            # Exclusión de entornos virtuales y secretos locales
├── .env.example             # Plantilla documentada de variables de entorno
├── .gitignore               # Exclusión de secretos, cachés y artefactos
├── db_config.py             # Configuración y validación dinámica de conexiones MySQL
├── Dockerfile               # Imagen de producción multi-stage (Python 3.10 + OpenJDK 17)
├── docker-compose.yml       # Orquestación local para la API y MySQL contenedorizado
├── etl_pipeline.py          # Lógica distribuida con PySpark (Extracción, ETL, Carga JDBC)
├── main.py                  # API REST FastAPI y ciclo de vida del servicio
├── Procfile                 # Descriptor de ejecución para despliegues PaaS
├── README.md                # Documentación técnica y guía de ejecución
└── requirements.txt         # Dependencias Python fijadas
```

---

## 4. Guía de Inicio Rápido para el Equipo

### Opción A: Ejecución con Docker Compose (Recomendada)

1. **Clonar el repositorio:**
   ```bash
   git clone <URL_REPOSITORIO>
   cd COVID-19
   ```

2. **Crear el archivo de configuración:**
   ```bash
   cp .env.example .env
   ```
   *Si vas a trabajar contra Aiven Cloud, completa las variables `AIVEN_DB_*` en tu `.env`.*

3. **Levantar los servicios:**
   * **Modo A (Conectado a Aiven Cloud):**
     ```bash
     docker compose up --build
     ```
   * **Modo B (100% Local con base de datos MySQL en Docker):**
     ```bash
     docker compose --profile with-local-db up --build
     ```

4. **Acceder a la aplicación:**
   Abre [http://localhost:8000](http://localhost:8000) en tu navegador para operar el flujo interactivo de 4 pasos:
   * **01 / Extracción:** Descarga el CSV consolidado de OWID (~400.000 filas) y lo lee con PySpark.
   * **02 / Transformación:** Limpia ubicaciones, descarta agregados continentales, imputa nulos y calcula el promedio móvil de 7 días y la tasa de letalidad.
   * **03 / Reseteo Aiven (Opcional):** Vence y recrea la estructura limpia de la tabla destino con índices optimizados.
   * **04 / Persistencia:** Inserta masivamente los datos en MySQL (Local o Aiven) mediante intercambio atómico (`RENAME TABLE`).

---

## 5. Variables de Entorno

| Variable | Requerida | Default | Descripción |
| :--- | :---: | :---: | :--- |
| `AIVEN_DB_HOST` | Sí (en la nube) | — | Host del clúster MySQL en Aiven Cloud |
| `AIVEN_DB_PORT` | Sí (en la nube) | `28590` | Puerto asignado en Aiven Cloud |
| `AIVEN_DB_USER` | Sí (en la nube) | `avnadmin` | Usuario administrador o de carga |
| `AIVEN_DB_PASSWORD` | Sí (en la nube) | — | Contraseña de MySQL en la nube |
| `AIVEN_DB_NAME` | Sí (en la nube) | `defaultdb` | Base de datos destino |
| `TABLE_RESET_TOKEN` | Opcional | — | Token secreto (mínimo 32 caracteres) para autorizar reseteos |
| `LOCAL_DB_HOST` | Sí (en local) | `localhost` | Host para persistencia local |
| `LOCAL_DB_PORT` | Sí (en local) | `3306` | Puerto para MySQL local |
| `LOCAL_DB_USER` | Sí (en local) | `root` | Usuario local |
| `LOCAL_DB_PASSWORD` | Sí (en local) | — | Contraseña local |
| `LOCAL_DB_NAME` | Sí (en local) | `covid19` | Base de datos local |
| `SPARK_DRIVER_MEMORY` | No | `2g` | Memoria RAM límite asignada al Driver de PySpark |
| `SPARK_EXECUTOR_MEMORY` | No | `1g` | Memoria RAM límite asignada a los Executors de PySpark |
| `SPARK_JDBC_BATCH_SIZE` | No | `10000` | Tamaño de lote de inserción en MySQL |
| `SPARK_LOG_LEVEL` | No | `INFO` | Nivel de logs de Spark (`INFO`, `WARN`, `ERROR`) |

---

## 6. Conexión de Power BI vía ODBC

1. Instalar el driver oficial **MySQL Connector/ODBC 8.4 (o 8.0) de 64 bits**.
2. En Windows, abrir `odbcad32.exe` (64 bits) y crear un **DSN de Sistema**:
   * **Data Source Name:** `Aiven_COVID19`
   * **TCP/IP Server:** Host de Aiven o Railway
   * **Port:** Puerto asignado
   * **User:** `avnadmin` (o tu usuario de lectura)
   * **Database:** `defaultdb`
   * **SSL Mode:** `REQUIRED`
3. En **Power BI Desktop**, seleccionar **Obtener datos $\rightarrow$ ODBC $\rightarrow$ Aiven_COVID19** y cargar la tabla `seguimiento_epidemiologico` en modo **Importar**.

---

## 7. Optimizaciones de Ingeniería Implementadas

* **Reescritura de Sentencias JDBC (`rewriteBatchedStatements=true`):** Convierte miles de llamadas individuales en operaciones multi-fila masivas, reduciendo el tiempo de transferencia WAN de más de 20 minutos a menos de 45 segundos.
* **Construcción Diferida de Índices:** La tabla temporal de staging se carga únicamente con la clave primaria, y los índices secundarios (`date` e `iso_code`) se construyen mediante un índice en bloque al finalizar la inserción masiva, evitando la contención en el árbol B-Tree de InnoDB.
* **Intercambio Atómico de Tablas (`Atomic Swap`):** El reemplazo de datos se efectúa con `RENAME TABLE actual TO backup, staging TO actual`, garantizando tiempo de inactividad cero (*zero-downtime*) y rollback en caso de fallo.
* **Control de Recursos en Docker:** Parámetros de memoria del contenedor y JVM balanceados para prevenir fallos por OOM (*Out Of Memory*) en entornos con límites de memoria de cgroups.

