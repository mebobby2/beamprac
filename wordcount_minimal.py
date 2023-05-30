import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import re
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

input_file = 'gs://dataflow-samples/shakespeare/kinglear.txt'
output_path = '/Users/BobbyLei/Desktop/learn/beamprac/data/wordcount_minimal_results.txt'

beam_options = PipelineOptions()

with beam.Pipeline(options=beam_options) as p:

  lines = p | ReadFromText(input_file)

  counts = (
    lines
    | 'Split' >> (
      beam.FlatMap(
        lambda x: re.findall(r'[A-Za-z\']+', x)).with_output_types(str))
    | 'LowerCase' >> beam.Map(lambda x: x.lower())
    | 'PairWithOne' >> beam.Map(lambda x: (x,1))
    | 'GroupAndSum' >> beam.CombinePerKey(sum)
  )

  def format_result(word_count):
    (word, count) = word_count
    return '%s: %s' % (word, count)

  output = counts | 'Format' >> beam.Map(format_result)

  output | WriteToText(output_path)
