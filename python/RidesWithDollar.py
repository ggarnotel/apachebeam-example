"""Taxi example python."""

from __future__ import absolute_import

import json
import argparse
import logging
import re

import six

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam import pvalue
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.examples.wordcount import WordExtractingDoFn
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions

class JsonCoder(object):
  """A JSON coder interpreting each line as a JSON string."""

  def encode(self, x):
    return json.dumps(x)

  def decode(self, x):
    return json.loads(x)

class Log(beam.DoFn):
  def process(self, element):
    logging.info(element)
    return element

def run(argv=None):
  """Main entry point; defines and runs the wordcount pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument('--output',
                      dest='output',
                      required=True,
                      help='Output file to write results to.')
  group = parser.add_mutually_exclusive_group(required=True)
  group.add_argument(
      '--input_topic',
      help=('Input PubSub topic of the form '
            '"projects/<PROJECT>/topics/<TOPIC>".'))
  group.add_argument(
      '--input_subscription',
      help=('Input PubSub subscription of the form '
            '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."'))
  known_args, pipeline_args = parser.parse_known_args(argv)


  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  #logging.info('average word length: %d', word_lengths_dist.committed.mean)
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  pipeline_options.view_as(StandardOptions).streaming = True

  p = beam.Pipeline(options=pipeline_options)

  if known_args.input_subscription:
    lines = p | beam.io.ReadStringsFromPubSub(
        subscription=known_args.input_subscription)
  else:
    lines = p | beam.io.ReadStringsFromPubSub(topic=known_args.input_topic)
  test = lines | 'LOG ua' >> beam.ParDo(Log())



  result = p.run()
  result.wait_until_finish()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
