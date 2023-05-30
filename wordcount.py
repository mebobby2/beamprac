import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import re
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

input_file = 'gs://dataflow-samples/shakespeare/kinglear.txt'
output_path = '/Users/BobbyLei/Desktop/learn/beamprac/data/wordcount_minimal_results.txt'

class WordExtract(beam.DoFn):
  def process(self, element):
    return re.findall(r'[\w\']+', element, re.UNICODE)

beam_options = PipelineOptions()

with beam.Pipeline(options=beam_options) as p:

  lines = p | ReadFromText(input_file)

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

  output | 'Write' >> WriteToText(output_path)
