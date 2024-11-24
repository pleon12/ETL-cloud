import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
import pandas as pd
from io import BytesIO
from google.cloud import storage


# Lecutra y transformación del xlsx
class ReadExcelFn(beam.DoFn):
    def __init__(self, bucket_name, file_name):
        self.bucket_name = bucket_name
        self.file_name = file_name

    def process(self, element):
        
        client = storage.Client()
        bucket = client.bucket(self.bucket_name)
        blob = bucket.blob(self.file_name)
        data = blob.download_as_bytes()

        
        df = pd.read_excel(BytesIO(data))

        # Convierte las filas del df a diccionarios para Apache Beam
        for _, row in df.iterrows():
            yield row.to_dict()


# Limpieza y pivoteo de datos
class CleanAndPivotFn(beam.DoFn):
    def process(self, element):
        # Extrae valores comunes
        año = int(element['Año'])
        entidad = element['Entidad'].strip()
        municipio = element['Municipio'].strip()
        bien_juridico = element['Bien jurídico afectado'].strip()
        tipo_delito = element['Tipo de delito'].strip()
        subtipo_delito = element['Subtipo de delito'].strip()
        modalidad = element['Modalidad'].strip()

        # Meses y casos
        meses = [
            "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
            "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"
        ]

        # Genera una fila por cada mes
        for mes in meses:
            casos = element.get(mes, None)
            if pd.notnull(casos) and isinstance(casos, (int, float)):  
                yield {
                    'año': año,
                    'entidad': entidad,
                    'municipio': municipio,
                    'bien_juridico': bien_juridico,
                    'tipo_delito': tipo_delito,
                    'subtipo_delito': subtipo_delito,
                    'modalidad': modalidad,
                    'mes': mes,
                    'casos': int(casos)
                }


def run():
    # Config pipeline
    options = PipelineOptions()
    gcp_options = options.view_as(GoogleCloudOptions)
    gcp_options.project = 'pipeline-incidencia-delictiva'
    gcp_options.region = 'us-central1'
    gcp_options.temp_location = 'gs://pipeline-incidencia-delictiva-bucket/temp'

    # Config runner
    standard_options = options.view_as(StandardOptions)
    standard_options.runner = 'DataflowRunner'

    # Config bucket y file
    bucket_name = 'pipeline-incidencia-delictiva-bucket'
    file_name = '2024.xlsx'

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Leer archivo XLSX' >> beam.ParDo(ReadExcelFn(bucket_name, file_name))
            | 'Limpiar y transformar datos' >> beam.ParDo(CleanAndPivotFn())
            | 'Escribir en BigQuery' >> beam.io.WriteToBigQuery(
                'pipeline-incidencia-delictiva:incidencia_delictiva.incidencia_raw',
                schema=(
                    'año:INTEGER,'
                    'entidad:STRING,'
                    'municipio:STRING,'
                    'bien_juridico:STRING,'
                    'tipo_delito:STRING,'
                    'subtipo_delito:STRING,'
                    'modalidad:STRING,'
                    'mes:STRING,'
                    'casos:INTEGER'
                ),
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
            )
        )


if __name__ == '__main__':
    run()
