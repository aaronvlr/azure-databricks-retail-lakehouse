Resumen del Proyecto ETL - Instacart Market Basket Analysis en Azure Databricks

Este proyecto fue desarrollado con el objetivo de diseñar e implementar un pipeline de datos integral (End-to-End) utilizando tecnologías de Big Data en la nube. El propósito principal es tomar un conjunto de datos masivo y real del supermercado Instacart, estructurarlo mediante buenas prácticas de ingeniería y disponibilizar métricas comerciales clave listas para la toma de decisiones empresariales.

IMAGEN 1

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

IMAGEN DEL OTRO CHAT

La arquitectura garantiza un aislamiento físico y lógico total. La gobernanza se administra estrictamente en Unity Catalog vinculando el almacenamiento a través de Storage Credentials (identidades administradas) y External Locations fijas asignadas a la raíz de cada contenedor del Data Lake.

Datasets de Origen (Instacart)

Se seleccionó el dataset público de Instacart disponible en Kaggle debido a su volumen (millones de filas) y a su estructura fiel a un modelo relacional de negocio:
Archivo plano (CSV),Descripción técnica,Tamaño en disco
order_products__train.csv,"Tabla de hechos. Registra qué productos pertenecen a qué orden, su posición de entrada al carrito y si es una recompra.",~24 MB (~1.3 millones de filas)
products.csv,Tabla de dimensión. Contiene el catálogo maestro de identificadores de productos asociados a sus nombres y categorías.,"~2.1 MB (~50,000 filas)"






    
