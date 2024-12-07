
# GCP Data Engineering Project: ETL Pipeline for Crime Incidence Data Analysis

Este proyecto de ingeniería de datos en GCP está enfocado en desarrollar un pipeline ETL (Extract, Transform, Load) para procesar, transformar y analizar datos de incidencia delictiva utilizando herramientas como Apache Beam y servicios de Google Cloud Platform (GCP).

## 🔧 Tecnologías utilizadas

- <img width="40" alt="image" src="https://beam.apache.org/images/mascot/beam_mascot_500x500.png"> **Apache Beam** para el procesamiento y transformación de datos.
- <img width="18" alt="image" src="https://www.odrive.com/images/links/logos/googlecloud.png"> **Google Cloud Storage (GCS)** como almacenamiento de datos.
- <img width="18" alt="image" src="https://symbols.getvecta.com/stencil_4/57_google-dataflow.aab763346e.png"> **Dataflow** como motor de procesamiento en la nube.
- <img width="18" alt="image" src="https://seeklogo.com/images/G/google-bigquery-logo-6E9BA2D0A3-seeklogo.com.png"> **BigQuery** como almacén de datos para consultas analíticas.
- <img width="18" alt="image" src="https://seeklogo.com/images/G/google-looker-logo-B27BD25E4E-seeklogo.com.png"> **Looker** para la visualización de resultados y generación de reportes.

## Arquitectura de solución
![ETL_diagram](https://github.com/user-attachments/assets/c54dd71a-c716-4742-acc7-59a9ce3716c7)

---

## 🗂️ Datos de entrada

El pipeline procesa un conjunto de datos de incidencia delictiva en formato Excel (xlsx). Este incluye información como:

- Año.
- Mes.
- Tipo y subtipo de delito.
- Modalidad.
- Bien jurídico afectado.
- Entidad y municipio junto con sus claves

Los datos corresponden a reportes reales de criminalidad y permiten extraer tendencias y patrones útiles para análisis posteriores.

---

## 🚀 Descripción del pipeline ETL

El código `beam_pipeline.py` implementa el pipeline ETL con las siguientes etapas:

1. **Extracción (Extract)**:
   - Lectura de datos desde un archivo excel alojado en un bucket de Google Cloud Storage.
   - Se lee el archivo excel usando la función de read_excel de pandas y después se convierten los registros a diccionarios usando un ciclo for para cada registro y aplicando la función 'to_dict'.

2. **Transformación (Transform)**:
   - Se crea una función para eliminar espacios en blanco al principio y al final de cada columna de tipo string, por otro lado, convierte en enteros (int) a la columna de año.
   - Se crea una función para convertir los meses que están en columnas a una sola columna de meses y se guardan el número de casos por mes en una columna llamada "casos" con un tipo de datos entero. Lo que se hace aquí es una tranformación de columnas a filas, a este proceso comúnmente se le conoce como pivoteo de datos o melt. 

3. **Carga (Load)**:
   - Los datos procesados se cargan a una tabla en BigQuery:
     - Tabla principal con los incidentes completos.

---

## 👩‍💻 Cómo ejecutar el pipeline

1. Configura tu proyecto en la nube:
   ```bash
   gcloud config set project [YOUR_PROJECT_ID]
   ```

2. Instala Apache Beam:
   ```bash
   pip install apache-beam[gcp]
   ```

3. Ejecuta el pipeline:
   ```bash
   python beam_pipeline.py \
   --input gs://[YOUR_BUCKET]/crime_data.csv \
   --temp_location gs://[YOUR_BUCKET]/temp \
   --output_bigquery_table [YOUR_PROJECT_ID]:[YOUR_DATASET].crime_incidents
   ```

4. Verifica los resultados en BigQuery y Looker.

---

## 💡 Orquestación con Composer (Airflow)

Para pipelines recurrentes, se puede usar **Google Cloud Composer** para orquestar tareas con Apache Airflow.

1. Configura un DAG que:
   - Monitoree el bucket de GCS para nuevos archivos.
   - Ejecute el pipeline de Dataflow al detectar datos nuevos.

2. Sube los archivos `dag_pipeline.py` y `beam_pipeline.py` al bucket del entorno Composer.

---

## 🖥️ Visualización en Looker

- Conecta BigQuery como fuente de datos en Looker.
- Crea dashboards que incluyan:
  - Mapas de calor por región.
  - Tendencias mensuales de incidencia delictiva.
  - Análisis por tipo de delito.

---

## 📊 Ejemplo de resultados

- **Incidentes procesados:** 50,000 registros.
- **Tendencia clave:** Incremento del 15% en delitos contra la propiedad en el último trimestre.
- **Análisis geográfico:** Las áreas urbanas concentran el 80% de los incidentes reportados.
