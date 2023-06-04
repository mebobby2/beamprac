import argparse
import csv
import logging
import sys
import time
from datetime import datetime

import apache_beam as beam
from apache_beam.metrics.metric import Metrics
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions

def timestamp2str(t, fmt='%Y-%m-%d %H:%M:%S.000'):
  """Converts a unix timestamp into a formatted string."""
  return datetime.fromtimestamp(t).strftime(fmt)

class ParseGameEvent(beam.DoFn):
  def __init__(self):
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
      self.num_parse_errors.inc()
      logging.error('Parse error on "%s"', elem)

class CalculateSpammyUsers(beam.PTransform):
  SCORE_WEIGHT = 2.5

  def expand(self, user_scores):
    sum_scores = (user_scores | 'SumUsersScores' >> beam.CombinePerKey(sum))

    # Extract the score from each element, and use it to find the global mean.
    global_mean_score = (
      sum_scores
      | beam.Values()
      | beam.CombineGlobally(beam.combiners.MeanCombineFn())\
          .as_singleton_view())

    # Filter the user sums using the global mean.
    filtered = (
      sum_scores
      # Use the derived mean total score (global_mean_score) as a side input.
      | 'ProcessAndFilter' >> beam.Filter(
        lambda key_score, global_mean:\
          key_score[1] > global_mean * self.SCORE_WEIGHT,
          global_mean_score))

    return filtered

def run(argv=None, save_main_session=True):
  parser = argparse.ArgumentParser()

  parser.add_argument('--topic', type=str, help='Pub/Sub topic to read from')
  parser.add_argument(
      '--subscription', type=str, help='Pub/Sub subscription to read from')
  parser.add_argument(
      '--fixed_window_duration',
      type=int,
      default=60,
      help='Numeric value of fixed window duration for user '
      'analysis, in minutes')
  parser.add_argument(
      '--session_gap',
      type=int,
      default=5,
      help='Numeric value of gap between user sessions, '
      'in minutes')
  parser.add_argument(
      '--user_activity_window_duration',
      type=int,
      default=30,
      help='Numeric value of fixed window for finding mean of '
      'user session duration, in minutes')

  args, pipeline_args = parser.parse_known_args(argv)

  if args.topic is None and args.subscription is None:
    parser.print_usage()
    print(sys.argv[0] + ': error: one of --topic or --subscription is required')
    sys.exit(1)

  options = PipelineOptions(pipeline_args)

  fixed_window_duration = args.fixed_window_duration * 60
  session_gap = args.session_gap * 60
  user_activity_window_duration = args.user_activity_window_duration * 60

  options.view_as(StandardOptions).streaming = True

  with beam.Pipeline(options=options) as p:
    if args.subscription:
      scores = p | 'ReadPubSub' >> beam.io.ReadFromPubSub(
          subscription=args.subscription)
    else:
      scores = p | 'ReadPubSub' >> beam.io.ReadFromPubSub(topic=args.topic)

    raw_events = (
      scores
      | 'DecodeString' >> beam.Map(lambda b: b.decode('utf-8'))
      | 'ParseGameEvent' >> beam.ParDo(ParseGameEvent())
      | 'AddEventTimestamps' >> beam.Map(lambda elem: beam.window.TimestampedValue(elem, elem['timestamp'])))

    # Extract username/score pairs from the event stream
    user_events = (
      raw_events
      | 'ExtractUserScores' >> beam.Map(lambda elem: (elem['user'], elem['score']))
    )

    # Calculate the total score per user over fixed windows, and cumulative
    # updates for late data
    spammers_view = (
      user_events
      | 'UserFixedWindows' >> beam.WindowInto(
        beam.window.FixedWindows(fixed_window_duration))

      # Filter out everyone but those with (SCORE_WEIGHT * avg) clickrate.
      # These might be robots/spammers.
      | 'CalculateSpammyUsers' >> CalculateSpammyUsers()

      # Derive a view from the collection of spammer users. It will be used as
      # a side input in calculating the team score sums, below
      | 'CreateSpammersView' >> beam.CombineGlobally(
        beam.combiners.ToDictCombineFn()).as_singleton_view())

    # [START filter_and_calc]
    # Calculate the total score per team over fixed windows, and emit cumulative
    # updates for late data. Uses the side input derived above --the set of
    # suspected robots-- to filter out scores from those users from the sum.


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
