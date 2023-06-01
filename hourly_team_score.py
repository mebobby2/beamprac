# HourlyTeamScore calculates the total score per team, per hour, in a fixed data set (such as one day’s worth of data).

# Rather than operating on the entire data set at once, HourlyTeamScore divides the input data into logical windows and performs calculations on those windows. This allows HourlyUserScore to provide information on scoring data per window, where each window represents the game score progress at fixed intervals in time (like once every hour).

# HourlyTeamScore filters data events based on whether their event time (as indicated by the embedded timestamp) falls within the relevant analysis period. Basically, the pipeline checks each game event’s timestamp and ensures that it falls within the range we want to analyze (in this case the day in question). Data events from previous days are discarded and not included in the score totals. This makes HourlyTeamScore more robust and less prone to erroneous result data than UserScore. It also allows the pipeline to account for late-arriving data that has a timestamp within the relevant analysis period.

# Limitation: HourlyTeamScore still has high latency between when data events occur (the event time) and when results are generated (the processing time), because, as a batch pipeline, it needs to wait to begin processing until all data events are present.

import argparse
import csv
import logging
import sys
import time
from datetime import datetime

import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.metrics.metric import Metrics
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

def str2timestamp(s, fmt='%Y-%m-%d-%H-%M'):
  dt = datetime.strptime(s, fmt)
  epoch = datetime.utcfromtimestamp(0)
  return (dt - epoch).total_seconds()


def timestamp2str(t, fmt='%Y-%m-%d %H:%M:%S.000'):
  return datetime.fromtimestamp(t).strftime(fmt)

class ExtractAndSumScore(beam.PTransform):
  def __init__(self, field):
    beam.PTransform.__init__(self)
    self.field = field

  def expand(self, pcoll):
    return (
        pcoll
        | beam.Map(lambda elem: (elem[self.field], elem['score']))
        | beam.CombinePerKey(sum))

class TeamScoresDict(beam.DoFn):
  def process(self, team_score, window=beam.DoFn.WindowParam):
    team, score = team_score
    start = timestamp2str(int(window.start))
    yield {
        'team': team,
        'total_score': score,
        'window_start': start,
        'processing_time': timestamp2str(int(time.time()))
    }

class ParseGameEvent(beam.DoFn):
  def __init__(self):
    # TODO(BEAM-6158): Revert the workaround once we can pickle super() on py3.
    # super().__init__()
    beam.DoFn.__init__(self)
    self.num_parse_errors = Metrics.counter(self.__class__, 'num_parse_errors')

  def process(self, elem):
    try:
      row = list(csv.reader([elem]))[0]
      yield {
          'user': row[0],
          'team': row[1],
          'score': int(row[2]),
          'timestamp': int(row[3]) / 1000.0,
      }
    except:  # pylint: disable=bare-except
      # Log and count parse errors
      self.num_parse_errors.inc()
      logging.error('Parse error on "%s"', elem)

class HourlyTeamScore(beam.PTransform):
  def __init__(self, start_min, stop_min, window_duration):
    # TODO(BEAM-6158): Revert the workaround once we can pickle super() on py3.
    # super().__init__()
    beam.PTransform.__init__(self)
    self.start_timestamp = str2timestamp(start_min)
    self.stop_timestamp = str2timestamp(stop_min)
    self.window_duration_in_seconds = window_duration * 60

  def expand(self, pcoll):
    return (
      pcoll
      | 'ParseGameEvent' >> beam.ParDo(ParseGameEvent())
      | 'FilterStartTime' >>
      beam.Filter(lambda elem: elem['timestamp'] > self.start_timestamp)
      | 'FilterEndTime' >>
      beam.Filter(lambda elem: elem['timestamp'] < self.stop_timestamp)
      | 'AddEventTimestamps' >> beam.Map(
        lambda elem: beam.window.TimestampedValue(elem, elem['timestamp'])
      )
      | 'FixedWindowsTeam' >> beam.WindowInto(
        beam.window.FixedWindows(self.window_duration_in_seconds)
      )
      | 'ExtractAndSumScore' >> ExtractAndSumScore('team')
    )

def run(argv=None, save_main_session=True):
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      type=str,
      default='gs://apache-beam-samples/game/gaming_data*.csv',
      help='Path to the data file(s) containing game data.')
  parser.add_argument(
      '--output', type=str, required=True, help='Path to the output file(s).')
  parser.add_argument(
      '--window_duration',
      type=int,
      default=60,
      help='Numeric value of fixed window duration, in minutes')
  parser.add_argument(
      '--start_min',
      type=str,
      default='1970-01-01-00-00',
      help='String representation of the first minute after '
      'which to generate results in the format: '
      'yyyy-MM-dd-HH-mm. Any input data timestamped '
      'prior to that minute won\'t be included in the '
      'sums.')
  parser.add_argument(
      '--stop_min',
      type=str,
      default='2100-01-01-00-00',
      help='String representation of the first minute for '
      'which to generate results in the format: '
      'yyyy-MM-dd-HH-mm. Any input data timestamped '
      'after to that minute won\'t be included in the '
      'sums.')

  args, pipeline_args = parser.parse_known_args(argv)
  options = PipelineOptions(pipeline_args)

  with beam.Pipeline(options=options) as p:
    (
      p
      | 'ReadInputText' >> beam.io.ReadFromText(args.input)
      | 'HourlyTeamScore' >> HourlyTeamScore(
        args.start_min, args.stop_min, args.window_duration
      )
      | 'TeamScoresDict' >> beam.ParDo(TeamScoresDict())
      | 'WriteTeamScoreSums' >> beam.io.fileio.WriteToFiles(path=args.output, shards=1, max_writers_per_bundle=0)
    )



if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
