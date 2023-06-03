import argparse
import json
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
from apache_beam.transforms import trigger

def timestamp2str(t, fmt='%Y-%m-%d %H:%M:%S.000'):
  return datetime.fromtimestamp(t).strftime(fmt)

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

class ExtractAndSumScore(beam.PTransform):
  def __init__(self, field):
    beam.PTransform.__init__(self)
    self.field = field

  def expand(self, pcoll):
    return (
        pcoll
        | beam.Map(lambda elem: (elem[self.field], elem['score']))
        | beam.CombinePerKey(sum))

class CalculateTeamScores(beam.PTransform):
  def __init__(self, team_window_duration, allowed_lateness):
    # TODO(BEAM-6158): Revert the workaround once we can pickle super() on py3.
    # super().__init__()
    beam.PTransform.__init__(self)
    self.team_window_duration = team_window_duration * 60
    self.allowed_lateness_seconds = allowed_lateness * 60

  def expand(self, pcoll):
    return (
      pcoll
      | 'LeaderboardTeamFixedWindows' >> beam.WindowInto(
        beam.window.FixedWindows(self.team_window_duration),
        trigger=trigger.AfterWatermark(
          trigger.AfterCount(10), trigger.AfterCount(20)
        ),
        accumulation_mode=trigger.AccumulationMode.ACCUMULATING
      )
      | 'ExtractAndSumScore' >> ExtractAndSumScore('team')
    )

class CalculateUserScores(beam.PTransform):
  def __init__(self, allowed_lateness):
    beam.PTransform.__init__(self)
    self.allowed_lateness_seconds = allowed_lateness * 60

  def expand(self, pcoll):
    return (
      pcoll
      | 'LeaderboardUserGlobalWindows' >> beam.WindowInto(
        beam.window.GlobalWindows(),
        trigger=trigger.Repeatedly(trigger.AfterCount(10)),
        accumulation_mode=trigger.AccumulationMode.ACCUMULATING)
      | 'ExtractAndSumScore' >> ExtractAndSumScore('user')
    )

def run(argv=None, save_main_session=True):
  parser = argparse.ArgumentParser()

  parser.add_argument('--topic', type=str, help='Pub/Sub topic to read from')
  parser.add_argument(
      '--subscription', type=str, help='Pub/Sub subscription to read from')
  parser.add_argument(
      '--team_window_duration',
      type=int,
      default=60,
      help='Numeric value of fixed window duration for team '
      'analysis, in minutes')
  parser.add_argument(
      '--allowed_lateness',
      type=int,
      default=120,
      help='Numeric value of allowed data lateness, in minutes')
  parser.add_argument(
      '--output', type=str, required=True, help='Path to the output file(s).')


  args, pipeline_args = parser.parse_known_args(argv)

  if args.topic is None and args.subscription is None:
    parser.print_usage()
    print(sys.argv[0] + ': error: one of --topic or --subscription is required')
    sys.exit(1)

  options = PipelineOptions(pipeline_args)

  # Enforce that this pipeline is always run in streaming mode
  options.view_as(StandardOptions).streaming = True

  with beam.Pipeline(options=options) as p:
    if args.subscription:
      scores = p | 'ReadPubSub' >> beam.io.ReadFromPubSub(subscription=args.subscription)
    else:
      scores = p | 'ReadPubSub' >> beam.io.ReadFromPubSub(topic=args.topic)

    events = (
      scores
      | 'DecodeString' >> beam.Map(lambda b: b.decode('utf-8'))
      | 'ParseGameEvent' >> beam.ParDo(ParseGameEvent())
      | 'AddEventTimestamps' >> beam.Map(
        lambda elem: beam.window.TimestampedValue(elem, elem['timestamp'])
      )
    )

    def format_team_score_dict(team_score):
      return 'team: %(team)s, total_score: %(total_score)s, window_start: %(window_start)s, processing_time: %(processing_time)s' % team_score

    (
      events
      | 'CalculateTeamScores' >> CalculateTeamScores(
        args.team_window_duration, args.allowed_lateness)
      | 'TeamScoresDict' >> beam.ParDo(TeamScoresDict())
      | 'FormatTeamScoresDict' >> beam.Map(format_team_score_dict)
      | 'WriteTeamScoreSums' >> beam.io.fileio.WriteToFiles(
          path=args.output,
          destination=lambda record: 'team_scores',
          file_naming=beam.io.fileio.destination_prefix_naming()
          )
    )

    def format_user_score_sums(user_score):
      (user, score) = user_score
      return 'user: %s, total_score: %s' % (user, score)

    (
      events
      | 'CalculateUserScores' >> CalculateUserScores(args.allowed_lateness)
      | 'FormatUserScoreSums' >> beam.Map(format_user_score_sums)
      | 'WriteUserScoreSums' >> beam.io.fileio.WriteToFiles(
          path=args.output,
          destination=lambda record: 'user_scores',
          file_naming=beam.io.fileio.destination_prefix_naming()
          )
    )


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
