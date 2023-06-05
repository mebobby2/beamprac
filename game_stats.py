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

class UserSessionActivity(beam.DoFn):
   def process(self, elem, window=beam.DoFn.WindowParam):
     yield (window.end.micros - window.start.micros) // 1000000

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
  parser.add_argument(
      '--output', type=str, required=True, help='Path to the output file(s).')

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
    def format_team_score_dict(team_score):
      return 'team: %(team)s, total_score: %(total_score)s, window_start: %(window_start)s, processing_time: %(processing_time)s' % team_score

    (
      raw_events
      | 'WindowIntoFixedWindows' >> beam.WindowInto(
          beam.window.FixedWindows(fixed_window_duration))

      # Filter out the detected spammer users, using the side input derived
      # above
      | ' FilterOutSpammers' >> beam.Filter(
        lambda elem, spammers: elem['user'] not in spammers, spammers_view)
      # Extract and sum teamname/score pairs from the event data.
      | 'ExtractAndSumScore' >> ExtractAndSumScore('team')
      | 'TeamScoresDict' >> beam.ParDo(TeamScoresDict())
      | 'FormatTeamScoresDict' >> beam.Map(format_team_score_dict)
      | 'WriteTeamScoreSums' >> beam.io.fileio.WriteToFiles(
          path=args.output,
          destination=lambda record: 'team_scores',
          file_naming=beam.io.fileio.destination_prefix_naming()
          )
    )

    # [START session_calc]
    # Detect user sessions-- that is, a burst of activity separated by a gap
    # from further activity. Find and record the mean session lengths.
    # This information could help the game designers track the changing user
    # engagement as their set of game changes.
    (
      user_events
      | 'WindowIntoSessions' >> beam.WindowInto(
          beam.window.Sessions(session_gap),
          timestamp_combiner=beam.window.TimestampCombiner.OUTPUT_AT_EOW)

      # For this use, we care only about the existence of the session, not any
      # particular information aggregated over it, so we can just group by key
      # and assign a "dummy value" of None.
      | beam.CombinePerKey(lambda _: None)

      # Get the duration of the session
      | 'UserSessionActivity' >> beam.ParDo(UserSessionActivity())

      # [START rewindow]
      # Re-window to process groups of session sums according to when the
      # sessions complete
      | 'WindowToExtractSessionMean' >> beam.WindowInto(
        beam.window.FixedWindows(user_activity_window_duration))

      # Find the mean session duration in each window
      | beam.CombineGlobally(
          beam.combiners.MeanCombineFn()).without_defaults()
      | 'FormatAvgSessionLength' >> beam.Map(lambda elem: 'mean_duration: %s' % elem)
      | 'WriteAvgSessionLength' >> beam.io.fileio.WriteToFiles(
          path=args.output,
          destination=lambda record: 'sessions',
          file_naming=beam.io.fileio.destination_prefix_naming()
          )
    )

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
