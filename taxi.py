import argparse
import logging

import apache_beam as beam
from apache_beam.examples.wordcount_with_metrics import WordExtractingDoFn
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.transforms import window
from apache_beam.io import WriteToText


def run(argv=None, save_main_session=True):
  """Build and run the pipeline."""
  parser = argparse.ArgumentParser()
  group = parser.add_mutually_exclusive_group(required=True)
  group.add_argument(
      '--input_topic',
      help=(
          'Input PubSub topic of the form '
          '"projects/<PROJECT>/topics/<TOPIC>".'))
  group.add_argument(
      '--input_subscription',
      help=(
          'Input PubSub subscription of the form '
          '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."'))
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  pipeline_options.view_as(StandardOptions).streaming = True
  with beam.Pipeline(options=pipeline_options) as p:

    # Read from PubSub into a PCollection.
    if known_args.input_subscription:
      messages = (
          p
          | beam.io.ReadFromPubSub(subscription=known_args.input_subscription).
          with_output_types(bytes))
    else:
      messages = (
          p
          | beam.io.ReadFromPubSub(
              topic=known_args.input_topic).with_output_types(bytes))

    # lines = messages | 'decode' >> beam.Map(lambda x: x.decode('utf-8'))

    # output = (
    #     messages
    #     | 'format' >> beam.Map(format_result)
    # )

    # Write to PubSub.
    # pylint: disable=expression-not-assigned
    # messages | beam.io.WriteToPubSub(known_args.output_topic)
    messages | 'Print' >> beam.Map(lambda x: print(x))



if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
