import apache_beam as beam

with beam.Pipeline() as pipeline:
    (
        pipeline
        | beam.Create(['Hola, Apache Beam!'])
        | beam.Map(print)
    )
