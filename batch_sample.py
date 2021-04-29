import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToBigQuery
from google.cloud import bigquery
from google.cloud import storage
import json
import re
import datetime
import argparse
import sys


class parseRow(beam.DoFn):
   def process(self, element, *args, **kwargs):
       list_fields=element.split(',')
       x={}
       x['price_timestamp']=datetime.datetime.strptime(list_fields[0],'%Y-%m-%d %H:%M:%S UTC')
       x['price_timestamp']= '2020-01-02 00:00:00'
       x['ad_type']=list_fields[1]
       x['price_100']=float(list_fields[2])
       x['price_1000']=float(list_fields[3])
       x['price_3000']=float(list_fields[4])
       # must be yield
       yield x

def parseSchema(gs_url):
   storage_client=storage.Client()
   blob_start=gs_url.rfind('/')
   blob_name=gs_url[-(len(gs_url)-blob_start-1):]
   bucket_name=gs_url[5:blob_start]
   bucket=storage_client.get_bucket(bucket_name)
   blob=bucket.get_blob(blob_name)
   data = json.loads(blob.download_as_string(client=None))
   field_array=data['BigQuery Schema']
   output_schema=''
   index = 0
   for field in field_array:
       field_name=field['name']
       field_type=field['type']
       field_str=field_name + ':' + field_type
       if index == 0:
           output_schema = field_str
           index = 1
       else:
           output_schema=','.join((output_schema,field_str))
   return output_schema


def run(argv):
   parser=argparse.ArgumentParser()
   # parameter project, temp_location is needed in command line, but can not be included in known_args.
   # parser.add_argument('--project',
   #                     dest='project',
   #                     required=True)
   parser.add_argument('--stagingLocation',
                       dest='stagingLocation',
                       required=True)
   parser.add_argument('--tempLocation',
                       dest='tempLocation',
                       required=True)
   parser.add_argument('--runner',
                       dest='runner',
                       required=True)
   parser.add_argument('--JSONPath',
                       dest='JSONPath',
                       required=True)
   parser.add_argument('--inputFilePattern',
                       dest='inputFilePattern',
                       required=True)
   parser.add_argument('--outputTable',
                       dest='outputTable',
                       required=True)
   parser.add_argument('--bigQueryLoadingTemporaryDirectory',
                       dest='bigQueryLoadingTemporaryDirectory',
                       required=True)


   known_args,pipeline_args=parser.parse_known_args(argv)
   pipeline_options=PipelineOptions(pipeline_args)
   pipeline_options.view_as(SetupOptions).save_main_session=True
   schema=parseSchema(known_args.JSONPath)
   p=beam.Pipeline(options=pipeline_options)
   (p
    | 'Read Text'>> beam.io.ReadFromText(known_args.inputFilePattern)
    | 'Split' >> beam.ParDo(parseRow())
    | 'Write to BQ' >> beam.io.WriteToBigQuery(
               known_args.outputTable,
               schema=schema,
               write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
               create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
           ))
   p.run().wait_until_finish()


if __name__ == '__main__':
   sys.exit(0) if run(sys.argv) else sys.exit(1)