Resumen del Proyecto ETL - Instacart Market Basket Analysis en Azure Databricks

Este proyecto fue desarrollado con el objetivo de diseñar e implementar un pipeline de datos integral (End-to-End) utilizando tecnologías de Big Data en la nube. El propósito principal es tomar un conjunto de datos masivo y real del supermercado Instacart, estructurarlo mediante buenas prácticas de ingeniería y disponibilizar métricas comerciales clave listas para la toma de decisiones empresariales.

<img width="1229" height="297" alt="imagen" src="https://github.com/user-attachments/assets/8974b54e-f8b3-4d85-8b20-114a0e4891df" />

El pipeline procesa millones de transacciones de compra y catálogos de alimentos utilizando PySpark en Azure Databricks, estructurando el almacenamiento bajo una arquitectura Medallion (Bronze → Silver → Golden) sobre contenedores independientes en Azure Data Lake Storage Gen2.

<img width="1024" height="559" alt="imagen" src="https://github.com/user-attachments/assets/2a9d8d85-c18d-40cb-8a68-ab57eaf1fa44" />

¿Qué se implementó en este proyecto?

A partir de archivos planos masivos con registros de carritos de compra y catálogos de productos, se construyó un ecosistema de datos robusto que limpia, enriquece y consolida la información para responder preguntas estratégicas de negocio (por ejemplo, analizar la cuota de mercado de productos orgánicos y la prioridad de selección de alimentos por departamento).
Para simular un entorno corporativo real, el proyecto abarca desde la preparación de infraestructura aislada en entornos de Desarrollo (DEV) y Producción (PROD), la gobernanza de datos con Unity Catalog mediante External Locations securizadas, la orquestación visual de jobs con rutas de mitigación de errores (Rollback automatizado), hasta el despliegue continuo de código (CI/CD) controlado por GitHub Actions.

Stack Tecnológico Utilizado

    Azure Databricks — Entorno unificado de ejecución y cómputo Spark.

    PySpark (Apache Spark) — Motor principal para procesamiento distribuido y transformaciones a gran escala.

    Delta Lake — Formato de almacenamiento open-source que aporta transacciones ACID y Time Travel al Data Lake.

    Unity Catalog — Herramienta de gobernanza centralizada para control de accesos, credenciales y auditoría.

    Azure Data Lake Storage Gen2 (ADLS Gen2) — Almacenamiento físico en la nube estructurado en entornos DEV y PROD.

    GitHub Actions — Orquestador de Integración y Despliegue Continuo (CI/CD) mediante flujos YAML.

    Databricks Dashboards — Motor nativo para reportería interactiva y visualización SQL.

Arquitectura diseñada

<img width="1024" height="558" alt="imagen" src="https://github.com/user-attachments/assets/1ed4c0eb-46b4-4f40-a589-37ccc57423d3" />

La arquitectura garantiza un aislamiento físico y lógico total. La gobernanza se administra estrictamente en Unity Catalog vinculando el almacenamiento a través de Storage Credentials (identidades administradas) y External Locations fijas asignadas a la raíz de cada contenedor del Data Lake.

Datasets de Origen (Instacart)

Se seleccionó el dataset público de Instacart disponible en Kaggle debido a su volumen (millones de filas) y a su estructura fiel a un modelo relacional de negocio:

| Archivo plano (CSV) | Descripción técnica | Tamaño en disco |
| :--- | :--- | :--- |
| `order_products__train.csv` | Tabla de hechos. Registra qué productos pertenecen a qué orden, su posición de entrada al carrito y si es una recompra. | ~24 MB (~1.3 millones de filas) |
| `products.csv` | Tabla de dimensión. Contiene el catálogo maestro de identificadores de productos asociados a sus nombres y categorías. | ~2.1 MB (~50,000 filas) |

Ambos archivos se cargaron inicialmente de forma manual en el contenedor raw del Storage Account de Azure para iniciar el flujo de ingesta.

Inicialización e Ingesta del Entorno (Raw a Bronze)

Todos los scripts SQL iniciales de gobernanza (creación de catálogos unificados, esquemas de base de datos, credenciales de almacenamiento y asignación de ubicaciones externas como exlt-raw, exlt-bronze, exlt-silver y exlt-golden) se encuentran centralizados en la carpeta de configuración del repositorio. Estos componentes actúan como pre-requisito técnico, aislando por completo las capas físicas y otorgando permisos de lectura y escritura seguros mediante Unity Catalog.

<img width="1264" height="500" alt="imagen" src="https://github.com/user-attachments/assets/56e3bd08-c661-445e-bda7-ae22b784eeb2" />

<img width="1274" height="561" alt="imagen" src="https://github.com/user-attachments/assets/1e5d6be7-3e7f-4323-bca7-2d779eeefea5" />

Durante la fase de ingesta (Notebooks de Bronze), PySpark lee los archivos CSV crudos aplicando esquemas explícitos definidos mediante StructType para optimizar el rendimiento y evitar el costo de inferencia. El DataFrame se enriquece agregando una columna de auditoría llamada ingestion_date con la marca de tiempo actual (current_timestamp()) y se persiste en formato Delta utilizando la validación posicional del comando .insertInto().

<img width="736" height="205" alt="imagen" src="https://github.com/user-attachments/assets/80ec5ff0-d1c0-4047-a6fb-a4c708b0c8d0" />

Orquestación y Resiliencia del Pipeline (Workflows) & Rollback (Reversion)

El flujo de procesamiento no depende de ejecuciones manuales ni de cuadernos maestros antiguos; se encuentra completamente automatizado mediante Databricks Workflows bajo el Job denominado ETL-PROJECT1. Las tareas se ejecutan siguiendo un Grafo Acíclico Dirigido (DAG) optimizado:

<img width="1168" height="207" alt="imagen" src="https://github.com/user-attachments/assets/302573fe-87a8-4b23-a3cd-49a0301ae053" />

Transformaciones Aplicadas (Bronze a Silver)

En la capa intermedia (Silver), el objetivo es unificar los datos crudos y aplicar lógica de negocio avanzada. Se lee de Bronze, se eliminan registros completamente nulos y se realiza un Inner Join optimizado a través de la llave común product_id. Durante este paso, se calcularon tres métricas analíticas clave:

1. is_organic (Inspección de texto): Evalúa mediante expresiones condicionales si la columna product_name contiene la palabra "Organic", clasificándolo estrictamente con una bandera comercial (Y / N).

2. cart_position_category (UDF de Spark): Se desarrolló y registró una función personalizada (User Defined Function) que categoriza la prioridad del usuario al hacer la compra en la app según la columna add_to_cart_order:

        First Pick: Si el producto fue el número 1 introducido al carrito.

        Essential: Si el producto se ubicó entre las posiciones 2 y 5.

        Add-on: Si se seleccionó después de la quinta posición.

3. reorder_desc (Mapeo de Atributos): Traduce el indicador binario nativo de recompra (reordered) hacia una descripción analítica explícita: 'Repeat Purchase' o 'New Purchase'.

Los campos resultantes se ordenan de manera exacta y secuencial para poblar de forma segura la tabla unificada de la capa Silver: catalog_au.silver.products_orders_enriched.

Modelo de Datos Analítico (Silver a Gold)

La capa final (Gold) está diseñada para la disponibilización de datos estructurados para el consumo de analistas de negocio y directores. Aquí, los datos atómicos a nivel de fila se reducen mediante agregaciones paralelas y se agrupan cruzando dos dimensiones de análisis fundamentales: department_id y la bandera is_organic.

La tabla resultante se denomina catalog_au.golden.kpis_sales_summary_partitioned y cuenta con la siguiente estructura:

| Columna | Tipo de dato | Descripción de KPI Comercial |
| :--- | :--- | :--- |
| `department_id` | `INTEGER` | Dimensión. Identificador único del departamento del supermercado. |
| `is_organic` | `STRING` | Dimensión / Llave de Partición. Bandera que indica si la categoría es orgánica (`Y`/`N`). |
| `total_orders` | `LONG` | Métrica. Volumen total de productos solicitados en esa combinación. |
| `total_reorders` | `LONG` | Métrica. Volumen total de productos que corresponden a recompras de clientes recurrentes. |
| `avg_add_to_cart_order` | `DOUBLE` | Métrica. Promedio del orden de inserción en el carrito (Prioridad de compra). |
| `ingestion_date` | `TIMESTAMP` | Auditoría. Marca de tiempo del procesamiento final. |

Mecanismo de Reversión (Rollback Automatizado)
Se creo un workflow de reversion, en caso de que saliera algo mal por problemas del codigo o falta de alguna configuracion no acorde a el objetivo. Este notebook se llama "Drop_medallion_reversion_GITHUB" se encarga de eliminar las tablas que se hayan creado de las capas medallion de bronze, silver y golden, ademas de tambien eliminar los external location creados a los containers en azure de bronze, silver y golden. 

Optimización de Rendimiento

Para maximizar la velocidad de las consultas analíticas y reportes interactivos, la tabla Gold fue físicamente estructurada en el Data Lake utilizando la cláusula PARTITIONED BY (is_organic). Esto divide los archivos Delta en directorios independientes según la bandera de orgánicos, forzando a Spark a leer únicamente las carpetas necesarias mediante el mecanismo de Partition Pruning.

Automatización y Despliegue Continuo (CI/CD)

El traslado de código y orquestación desde el entorno de Desarrollo hacia el Workspace de Producción está completamente automatizado a través de un pipeline en GitHub Actions (.github/workflows/script3.yml). Al realizar un push a la rama main, el flujo ejecuta secuencialmente:

1. Creacion de secrets: Los secrets que configuré en el repositorio con un periodo o vigencia de 90 dias (mas que suficiente para entregar proyecto):

*DATABRICKS_ORIGIN_HOST — URL del workspace de construcción
*DATABRICKS_ORIGIN_TOKEN — token del workspace de construcción
*DATABRICKS_DEST_HOST — URL del workspace de producción
*DATABRICKS_DEST_TOKEN — token del workspace de producción

2. Extracción en DEV: Exporta en caliente los notebooks en formato plano desde el espacio de desarrollo.
3. Despliegue en PROD: Importa y sobreescribe los notebooks en la ruta compartida de producción (/Workspace/Production/trabajo_final/proceso).
4. Búsqueda Dinámica de Infraestructura: Invoca la API de clusters de Databricks para localizar el clúster activo de producción por su nombre (cluster_SD) y extrae su ID dinámico.
5. Recreación del Job: Inyecta el ID del clúster de producción y el JSON detallado de las 6 tareas reales del Workflow, actualizando o recreando el Job en el entorno productivo de forma transparente.

Capa de Visualización Analítica (Dashboard Lakeview)

Para consumir la información de la tabla Golden, se construyó un Dashboard interactivo nativo dentro de Databricks estructurado mediante dos consultas SQL optimizadas:

* Query Principal (Métricas y Gráficos): Construye los KPIs de negocio traduciendo en tiempo real mediante una sentencia CASE WHEN los 21 IDs de departamentos reales de Instacart hacia nombres comerciales legibles (por ejemplo: 'Congelados', 'Lácteos y Huevos', 'Fiambrería y Deli'). Alimentando los componentes visuales.

<img width="994" height="868" alt="imagen" src="https://github.com/user-attachments/assets/c09f8b07-a2df-4ebf-a233-7ff96ad92a1b" />

<img width="1671" height="843" alt="imagen" src="https://github.com/user-attachments/assets/1c9bd6cb-a5e0-456c-a50b-dc1c284b180b" />

Autor
Proyecto desarrollado e implementado por Aaron Vargas como entregable final y aprendizaje en ingeniería de datos con Azure Databricks.
