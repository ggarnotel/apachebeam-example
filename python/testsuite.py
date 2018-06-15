#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""A word-counting workflow."""

from __future__ import absolute_import

import json
import argparse
import logging
import re

import six

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

class JsonCoder(object):
  """A JSON coder interpreting each line as a JSON string."""

  def encode(self, x):
    return json.dumps(x)

  def decode(self, x):
    return json.loads(x)

class Populate(beam.DoFn):

  def process(self, element):
    events = element['events']
    monitors = element['monitors']
    info = element['info']
    testsuites = element['testsuites']
    useractions = element['useractions']
    campaign = element['campaign']
    

    for event in events:
      yield event

    for monitor in monitors:
      yield pvalue.TaggedOutput('monitorsPC', monitor)
    for testsuite in testsuites:
      yield pvalue.TaggedOutput('testsuitesPC', testsuite)
    for useraction in useractions:
      yield pvalue.TaggedOutput('useractionsPC', useraction)
    for campaignu in campaign:
      yield pvalue.TaggedOutput('campaignPC', campaignu)
    yield pvalue.TaggedOutput('infoPC', info)

class KVForUAByName(beam.DoFn):
  def __init__(self, key_column):
      self._key_column = key_column

  def process(self, element):
    (key, values) = element
    newRow = {}
    newRow["uniqTcId"] = key
    for value in values:
      if self._key_column in value:
        newRow["type"] = value[self._key_column]
        newRow["parameter"] = value
      else:
        newRow["result"] = value
    yield newRow["type"],newRow

class SortAndComplete(beam.DoFn):
  def process(self, element):
    values = element[1]
    values = sorted(values, key=lambda k: k['tStampMs'])
    i=0
    while i < len(values):
      valueN = values[i]
      if i+1 != len(values):
        valueN['tStampMsEnd'] = values[i+1]['tStampMs']
      i+=1
    yield element

class Log(beam.DoFn):
  def process(self, element):
    logging.info(element)
    return element

class LogLen(beam.DoFn):
  def process(self, element):
    logging.info(len(element))
    return element

def filter_event(key, value):
    logging.info(key)

def KpiEventUserActionbeam(event, ua):
    logging.info("UA "+ua + "  EVENT " + event)
    yield event

def run(argv=None):
  """Main entry point; defines and runs the wordcount pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument('--input',
                      dest='input',
                      default='gs://dataflow-samples/shakespeare/kinglear.txt',
                      help='Input file to process.')
  parser.add_argument('--output',
                      dest='output',
                      required=True,
                      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  #logging.info('average word length: %d', word_lengths_dist.committed.mean)
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  p = beam.Pipeline(options=pipeline_options)

  # Read the text file[pattern] into a PCollection.
  lines = p | 'read' >> ReadFromText(known_args.input, coder=JsonCoder())
  
  results = (lines
              | 'Populate' >> beam.ParDo(Populate()).with_outputs('monitorsPC', 'infoPC', 'testsuitesPC', 'useractionsPC', 'campaignPC',main='eventsPC'))

  useractionsPColl = results.useractionsPC
  useractionsKPI = (useractionsPColl
              | 'KV UA uniqTcId' >> beam.Map(lambda x: (x['uniqTcId'], x))
              | 'group UA uniqTcId' >> beam.GroupByKey()
              | 'KV UA name' >> beam.ParDo(KVForUAByName("name"))
              | 'group UA name' >> beam.GroupByKey()
              | 'Filter TestFileTransfer' >> beam.Filter(lambda (key, values): key=='TestFileTransfer')
              | 'LOG ua' >> beam.ParDo(LogLen()))


  eventsPColl = results.eventsPC
  eventGroup = (eventsPColl
              | 'KV Event' >> beam.Map(lambda x: (x['name'], x))
              | 'group Event' >> beam.GroupByKey())

  eventKPI = (eventGroup
              | 'Filter batt' >> beam.Filter(lambda (key, values): key=='batteryLevelRemaining')
#              | 'LOG event' >> beam.ParDo(Log())
              )

  eventSort = (eventGroup
              | 'SortAndComplete' >> beam.ParDo(SortAndComplete())
              | 'Filter RAT' >> beam.Filter(lambda (key, values): key=='psRatChd')
              | 'KpiEventUserAction' >> beam.FlatMap(KpiEventUserActionbeam,ua=pvalue.AsSingleton(useractionsKPI))
              )
  '''


  
  monitorsPColl = results.monitorsPC
  monitorsKPI = (monitorsPColl
              | 'LOG monitors' >> beam.ParDo(Log()))
  infoPColl = results.infoPC
  infoKPI = (infoPColl
              | 'LOG info' >> beam.ParDo(Log()))
  testsuitesPColl = results.testsuitesPC
  testsuitesKPI = (testsuitesPColl
              | 'LOG testsuite' >> beam.ParDo(Log()))
  campaignPColl = results.campaignPC
  campaignKPI = (campaignPColl
              | 'LOG campaign' >> beam.ParDo(Log()))
  '''

  result = p.run()
  result.wait_until_finish()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
