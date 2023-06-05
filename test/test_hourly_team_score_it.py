import logging
import unittest

import pytest
from hamcrest.core.core.allof import all_of

import hourly_team_score
from apache_beam.io.gcp.tests import utils
from apache_beam.io.gcp.tests.bigquery_matcher import BigqueryMatcher
from apache_beam.runners.runner import PipelineState
from apache_beam.testing.pipeline_verifiers import PipelineStateMatcher
from apache_beam.testing.test_pipeline import TestPipeline


class HourlyTeamScoreIT(unittest.TestCase):

  DEFAULT_INPUT_FILE = 'gs://dataflow-samples/game/gaming_data*'
  # SHA-1 hash generated from sorted rows reading from BigQuery table
  DEFAULT_EXPECTED_CHECKSUM = '4fa761fb5c3341ec573d5d12c6ab75e3b2957a25'
  OUTPUT_DATASET = 'hourly_team_score_it_dataset'
  OUTPUT_TABLE = 'leader_board'

  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)
    self.project = self.test_pipeline.get_option('project')

    # Set up BigQuery environment
    self.dataset_ref = utils.create_bq_dataset(
        self.project, self.OUTPUT_DATASET)

  @pytest.mark.it_postcommit
  def test_hourly_team_score_it(self):
    state_verifier = PipelineStateMatcher(PipelineState.DONE)
    query = (
        'SELECT COUNT(*) FROM `%s.%s.%s`' %
        (self.project, self.dataset_ref.dataset_id, self.OUTPUT_TABLE))

    bigquery_verifier = BigqueryMatcher(
        self.project, query, self.DEFAULT_EXPECTED_CHECKSUM)

    extra_opts = {
        'input': self.DEFAULT_INPUT_FILE,
        'dataset': self.dataset_ref.dataset_id,
        'window_duration': 1,
        'on_success_matcher': all_of(state_verifier, bigquery_verifier)
    }

    # Register clean up before pipeline execution
    # Note that actual execution happens in reverse order.
    self.addCleanup(utils.delete_bq_dataset, self.project, self.dataset_ref)

    # Get pipeline options from command argument: --test-pipeline-options,
    # and start pipeline job by calling pipeline main function.
    hourly_team_score.run(
        self.test_pipeline.get_full_options_as_args(**extra_opts),
        save_main_session=False)

  @pytest.mark.no_xdist
  @pytest.mark.examples_postcommit
  def test_hourly_team_score_output_checksum_on_small_input(self):
    # Small dataset to prevent Out of Memory when running in local runners
    INPUT_FILE = 'gs://apache-beam-samples/game/small/gaming_data.csv'
    EXPECTED_CHECKSUM = '91143e81622aa391eb62eaa3f3a5123401edb07d'
    state_verifier = PipelineStateMatcher(PipelineState.DONE)
    query = (
        'SELECT COUNT(*) FROM `%s.%s.%s`' %
        (self.project, self.dataset_ref.dataset_id, self.OUTPUT_TABLE))

    bigquery_verifier = BigqueryMatcher(self.project, query, EXPECTED_CHECKSUM)

    extra_opts = {
        'input': INPUT_FILE,
        'dataset': self.dataset_ref.dataset_id,
        'window_duration': 1,
        'on_success_matcher': all_of(state_verifier, bigquery_verifier)
    }

    # Register clean up before pipeline execution
    # Note that actual execution happens in reverse order.
    self.addCleanup(utils.delete_bq_dataset, self.project, self.dataset_ref)

    # Get pipeline options from command argument: --test-pipeline-options,
    # and start pipeline job by calling pipeline main function.
    hourly_team_score.run(
        self.test_pipeline.get_full_options_as_args(**extra_opts))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
