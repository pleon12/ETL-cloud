import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
import pandas as pd
from io import BytesIO
from google.cloud import storage


class ReadAndPrepareDataFn(beam.DoFn):
    def __init__(self, bucket_name, file_name):
        self.bucket_name = bucket_name
        self.file_name = file_name

    def process(self, element):
        # Descarga  desde GCS
        client = storage.Client()
        bucket = client.bucket(self.bucket_name)
        blob = bucket.blob(self.file_name)
        data = blob.download_as_bytes()

      
        df = pd.read_csv(BytesIO(data))

        # Columna Ticket se crea como Serie+Documento
        df['Ticket'] = df['Serie'].astype(str) + df['Documento'].astype(str)

        Base = df[['Ticket', 'ProductoId', 'Descripcion', 'Cantidad', 'Utilidad']].copy()

        # Cálculo de Frecuencia_Normalizada
        grupo = Base.groupby('ProductoId')['Cantidad'].sum().reset_index()
        max_frecuencia = grupo['Cantidad'].max()
        min_frecuencia = grupo['Cantidad'].min()
        grupo['Frecuencia_Normalizada'] = grupo['Cantidad'].apply(
            lambda x: 1 + 4 * (x - min_frecuencia) / (max_frecuencia - min_frecuencia) if max_frecuencia != min_frecuencia else 3
        )
        Base = Base.merge(grupo[['ProductoId', 'Frecuencia_Normalizada']], on='ProductoId', how='left')

        # Ajuste del promedio en utilidad
        promedio_utilidad = Base.groupby('ProductoId')['Utilidad'].mean().reset_index()
        promedio_utilidad.rename(columns={'Utilidad': 'Promedio_Utilidad'}, inplace=True)
        Base = Base.merge(promedio_utilidad[['ProductoId', 'Promedio_Utilidad']], on='ProductoId', how='left')
        Base['Utilidad'] = Base['Promedio_Utilidad']
        Base.drop(columns=['Promedio_Utilidad'], inplace=True)

        # Agrupar por Ticket
        grouped = Base.groupby('Ticket').apply(lambda x: x.to_dict(orient='records')).reset_index(name='Productos')

        # Output: Productos en cada ticket
        for _, row in grouped.iterrows():
            yield {
                'Ticket': row['Ticket'],
                'Productos': row['Productos']  
            }


def run():
    # Config pipeline
    options = PipelineOptions()
    gcp_options = options.view_as(GoogleCloudOptions)
    gcp_options.project = 'pipeline-incidencia-delictiva'  # Hay que ver si dejamos el mismo id de proyecto o creamos uno nuevo
    gcp_options.region = 'us-central1'
    gcp_options.temp_location = 'gs://pipeline-incidencia-delictiva-bucket/temp'

    # Config runner
    standard_options = options.view_as(StandardOptions)
    standard_options.runner = 'DirectRunner'  

    # Config del bucket y el file
    bucket_name = 'pipeline-incidencia-delictiva-bucket'
    file_name = 'Base con utilidad.csv'

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Leer y preparar datos' >> beam.ParDo(ReadAndPrepareDataFn(bucket_name, file_name))
            | 'Escribir en BigQuery' >> beam.io.WriteToBigQuery(
                'pipeline-incidencia-delictiva:market_basket.transactions',
                schema=(
                    'Ticket:STRING,'
                    'Productos:STRING'
                ),
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
            )
        )


if __name__ == '__main__':
    run()
