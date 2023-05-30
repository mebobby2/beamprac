import apache_beam as beam
import argparse
import re
import logging

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

class WordExtract(beam.DoFn):
  def process(self, element):
    return re.findall(r'[\w\']+', element, re.UNICODE)

def run(argv=None, save_main_session=True):
  parser = argparse.ArgumentParser()
  parser.add_argument(
    '--input',
    dest='input',
    default='gs://dataflow-samples/shakespeare/kinglear.txt',
    help='Input file to process.'
  )
  parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)

  with beam.Pipeline(options=pipeline_options) as p:

    lines = p | ReadFromText(known_args.input)

    counts = (
      lines
      | 'Split' >> (beam.ParDo(WordExtract()).with_output_types(str))
      | 'LowerCase' >> beam.Map(lambda x: x.lower())
      | 'PairWithOne' >> beam.Map(lambda x: (x,1))
      | 'GroupAndSum' >> beam.CombinePerKey(sum)
    )

    def format_result(word, count):
      return '%s: %s' % (word, count)

    output = counts | 'Format' >> beam.MapTuple(format_result)

    output | 'Write' >> WriteToText(known_args.output)

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
