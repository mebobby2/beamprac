"""End-to-end test for the user score example.

Code: beam/sdks/python/apache_beam/examples/complete/game/user_score.py
Usage:

    pytest --test-pipeline-options=" \
      --runner=TestDataflowRunner \
      --project=... \
      --region=... \
      --staging_location=gs://... \
      --temp_location=gs://... \
      --output=gs://... \
      --sdk_location=... \

"""

import logging
import unittest
import uuid

import pytest
from hamcrest.core.core.allof import all_of
import user_score

from apache_beam.runners.runner import PipelineState
from apache_beam.testing.pipeline_verifiers import FileChecksumMatcher
from apache_beam.testing.pipeline_verifiers import PipelineStateMatcher
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_utils import delete_files


class UserScoreIT(unittest.TestCase):

  DEFAULT_INPUT_FILE = 'gs://dataflow-samples/game/gaming_data*'
  DEFAULT_OUTPUT_FILE = \
      'gs://temp-storage-for-end-to-end-tests/py-it-cloud/output'
  DEFAULT_EXPECTED_CHECKSUM = '9f3bd81669607f0d98ec80ddd477f3277cfba0a2'

  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)
    self.uuid = str(uuid.uuid4())

    self.output = '/'.join([self.DEFAULT_OUTPUT_FILE, self.uuid, 'results'])

  @pytest.mark.it_postcommit
  def test_user_score_it(self):

    state_verifier = PipelineStateMatcher(PipelineState.DONE)
    arg_sleep_secs = self.test_pipeline.get_option('sleep_secs')
    sleep_secs = int(arg_sleep_secs) if arg_sleep_secs is not None else None
    file_verifier = FileChecksumMatcher(
        self.output + '/*-of-*', self.DEFAULT_EXPECTED_CHECKSUM, sleep_secs)

    extra_opts = {
        'input': self.DEFAULT_INPUT_FILE,
        'output': self.output + '/user-score',
        'on_success_matcher': all_of(state_verifier, file_verifier)
    }

    # Register clean up before pipeline execution
    self.addCleanup(delete_files, [self.output + '*'])

    # Get pipeline options from command argument: --test-pipeline-options,
    # and start pipeline job by calling pipeline main function.
    user_score.run(
        self.test_pipeline.get_full_options_as_args(**extra_opts),
        save_main_session=False)

  @pytest.mark.no_xdist
  @pytest.mark.examples_postcommit
  def test_userscore_output_checksum_on_small_input(self):
    # Small dataset to prevent Out of Memory when running in local runners
    INPUT_FILE = 'gs://apache-beam-samples/game/small/gaming_data.csv'
    EXPECTED_CHECKSUM = '5b1bc0e8080e3c0f162809ac4c0f49acab23854e'
    state_verifier = PipelineStateMatcher(PipelineState.DONE)
    arg_sleep_secs = self.test_pipeline.get_option('sleep_secs')
    sleep_secs = int(arg_sleep_secs) if arg_sleep_secs is not None else None
    file_verifier = FileChecksumMatcher(
        self.output + '/*-of-*', EXPECTED_CHECKSUM, sleep_secs)

    extra_opts = {
        'input': INPUT_FILE,
        'output': self.output + '/user-score',
        'on_success_matcher': all_of(state_verifier, file_verifier)
    }

    # Register clean up before pipeline execution
    self.addCleanup(delete_files, [self.output + '*'])

    # Get pipeline options from command argument: --test-pipeline-options,
    # and start pipeline job by calling pipeline main function.
    user_score.run(self.test_pipeline.get_full_options_as_args(**extra_opts))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
