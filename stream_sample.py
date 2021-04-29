import apache_beam as beam
import argparse
import apache_beam.transforms.window as window
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
import re
from past.builtins import unicode
import logging
import sys
from time import gmtime,strftime
from datetime import datetime,timezone,timedelta

class extractWord(beam.DoFn):
   def process(self, element, *args, **kwargs):
       return re.findall(r'[\w]+',element,re.UNICODE)

def count_ones(word_ones):
   (word,ones)=word_ones
   return (word,sum(ones))

def format_result_for_pubsub(word_count):
     (word, count) = word_count
     return '%s: %d' % (word, count)

class format_result_for_bq(beam.DoFn):
 def process(self, element, window=beam.DoFn.WindowParam):
   ts_format = '%Y-%m-%d %H:%M:%S.%f UTC'
   window_start = window.start.to_utc_datetime().strftime(ts_format)
   window_end = window.end.to_utc_datetime().strftime(ts_format)
   # process_time=datetime.now().replace(tzinfo=timezone.utc).strftime(ts_format)
   process_time = (datetime.now() - timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S UTC')
   return [
       {
           'word': element[0],
           'count': element[1],
           'window_start': window_start,
           'window_end': window_end,
           'process_time':process_time,
           # 'row_index':1
      }
       # This is to write the second record to db, just to best this function
       # ,{
       #     'word': element[0],
       #     'count': element[1],
       #     'window_start': window_start,
       #     'window_end': window_end,
       #     'process_time':process_time,
       #     'row_index': 2
       # }
   ]

def run(argv=None,save_main_session=True):
   parser=argparse.ArgumentParser()
   parser.add_argument('--outputTable',
                       dest='outputTable',
                       required=True)
   parser.add_argument('--stagingLocation',
                       dest='stagingLocation',
                       required=True)
   parser.add_argument('--tempLocation',
                       dest='tempLocation',
                       required=True)
   parser.add_argument('--runner',
                       dest='runner',
                       required=True)
   # parser.add_argument('--streaming',
   #                     dest='streaming',
   #                     required=True)

   group=parser.add_mutually_exclusive_group(required=True)
   group.add_argument('--inputTopic',
                       dest='inputTopic')
   group.add_argument('--inputSub',
                       dest='inputSub')

   known_args,pipeline_args=parser.parse_known_args(argv)
   pipeline_options=PipelineOptions(pipeline_args)
   pipeline_options.view_as(SetupOptions).save_main_session=save_main_session
   pipeline_options.view_as(StandardOptions).streaming=True

   p=beam.Pipeline(runner=known_args.runner,options=pipeline_options)
   if known_args.inputSub:
       message=(
           p
           |beam.io.ReadFromPubSub(subscription=known_args.inputSub))
   else:
       message=(
           p
           |beam.io.ReadFromPubSub(topic=known_args.inputTopic)
       )
   lines=message|'decode' >> beam.Map(lambda x:x.decode('utf-8'))
   counts=(
       lines
       |'split'>>beam.ParDo(extractWord()).with_output_types(unicode)
       # |'split'>>beam.FlatMap(lambda line: re.findall(r'[\w]+',line)).with_output_types(unicode)
       # |'split'>> beam.FlatMap(lambda line: re.findall(r'[\w]+', line)).with_output_types(str)
       |'pair with 1' >> beam.Map(lambda x:(x,1))
       |beam.WindowInto(window.FixedWindows(15,0))
       |'group' >> beam.GroupByKey()
       |'count' >> beam.Map(count_ones)
       |'format output' >> beam.ParDo(format_result_for_bq())
       |'write to bq' >> beam.io.WriteToBigQuery(
           known_args.outputTable,
           schema=('word:STRING, count:INTEGER, window_start:TIMESTAMP, window_end:TIMESTAMP,process_time:TIMESTAMP,row_index:INTEGER'),
           create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
           write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
       )
   )
   p.run().wait_until_finish()

if __name__ == '__main__':


   logging.getLogger().setLevel(logging.INFO)
   run()
