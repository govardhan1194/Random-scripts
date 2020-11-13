import apache_beam as beam
import logging
import argparse

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.io.kafka import ReadFromKafka



def run(argv=None):
    '''Main method for executing the pipeline operation
    '''
    parser = argparse.ArgumentParser()

    parser.add_argument('--input_mode',
                        default='stream',
                        help='Streaming input or file based batch input')

    parser.add_argument('--input_topic',
                        required=True,
                        help='Topic to pull data from.')
                                            
    parser.add_argument(
        '--bootstrap_servers',
        dest='bootstrap_servers',
        required=True,
        help='Bootstrap servers for the Kafka cluster. Should be accessible by '
        'the runner')

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    if known_args.input_mode == 'stream':
        pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=pipeline_options) as p:

        pcol = (
        p
        | ReadFromKafka(
            consumer_config={'bootstrap.servers': known_args.bootstrap_servers},
            topics=known_args.input_topic)
        | "print" >> beam.Map(print)
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()








