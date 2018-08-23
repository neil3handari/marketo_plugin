from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator, SkipMixin

from MarketoPlugin.hooks.marketo_hook import MarketoHook
from MarketoPlugin.schemas._schema import schema

import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

import boa
import json
import logging
from tempfile import NamedTemporaryFile
from time import sleep
from csv import reader


class MarketoToS3Operator(BaseOperator, SkipMixin):
    """
    NOTE: The only currently supported + tested output format is json.
    There are references to avro in this code but support for that format
    had to be delayed.

    Marketo to S3 Operator
    :param marketo_conn_id:         The Airflow connection id used to store
                                    the Marketo credentials.
    :type marketo_conn_id:          string
    :param marketo_endpoint:        The endpoint to retreive data for. Possible
                                    values include:
                                                    - activities
                                                    - campaigns
                                                    - leads
                                                    - programs
                                                    - lead_lists
    :type marketo_endpoint:         string
    :type start_at:                 The starting date parameter. Ignored by all
                                    endpoints but leads.
    :param start_at:                Isoformat timestamp
    :type end_at:                   The ending date parameter. Ignored by all
                                    endpoints but leads.
    :type end_at:                   Isoformat timestamp
    :param payload:                 Payload variables -- all are optional
    :param output_format:           The output format of the data. Possible
                                    values include:
                                        - json
                                        - avro
                                    Defaults to json.
    :type output_format             string
    :param s3_conn_id:              The Airflow connection id used to store
                                    the S3 credentials.
    :type s3_conn_id:               string
    :param s3_bucket:               The S3 bucket to be used to store
                                    the Marketo data.
    :type s3_bucket:                string
    :param s3_key:                  The S3 key to be used to store
                                    the Marketo data.
    :type s3_bucket:                string
    """

    template_fields = ('payload',
                       's3_key',
                       'start_at',
                       'end_at')

    def __init__(self,
                 marketo_conn_id,
                 endpoint,
                 s3_conn_id,
                 s3_bucket,
                 s3_key,
                 output_format='json',
                 start_at=None,
                 end_at=None,
                 payload={},
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.marketo_conn_id = marketo_conn_id
        self.endpoint = endpoint.lower()
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.output_format = output_format.lower()
        self.start_at = start_at
        self.end_at = end_at
        self.payload = payload

        if self.endpoint.lower() not in ('activities',
                                         'campaigns',
                                         'leads',
                                         'programs',
                                         'lead_lists'):

            raise Exception('Specified endpoint not currently supported.')

        if self.output_format not in ('json'):
            raise Exception('Specified output format not currently supported.')
    def execute(self, context):
        self.token = (MarketoHook(http_conn_id=self.marketo_conn_id)
                      .run(self.methodMapper('auth'))
                      .json())['access_token']
        if self.endpoint == 'activities':
            paging_token = self.paginate_data(endpoint='paging_token',
                                              payload={'sinceDatetime': '2014-01-01T00:00:00'})
            activity_types = self.paginate_data(endpoint='activity_types')
            activities = [activity['id'] for activity in activity_types]
            output = []
            output += self.paginate_data(payload={'activityTypeIds': activities[0],
                                                  'nextPageToken': paging_token})
        elif self.endpoint == 'leads':
            request = {}
            lead_fields = self.paginate_data(endpoint='lead_description')
            request['fields'] = []
            for record in lead_fields:
                try:
                    request['fields'].append(record['rest']['name'])
                except:
                    pass
                    
            request['columnHeaderNames'] = {field : field for field in request['fields']}
      
            request['filter'] = {}
            createdAt = {}
            createdAt['startAt'] = self.start_at
            createdAt['endAt'] = self.end_at
            request['filter']['updatedAt'] = createdAt
            request['format'] = 'CSV'
            get_hook = MarketoHook(http_conn_id=self.marketo_conn_id)

            post_hook = MarketoHook(method='POST',
                                    http_conn_id=self.marketo_conn_id)

            job = post_hook.run(self.methodMapper('leads_create'),
                                data=json.dumps(request),
                                token=self.token).json()
            export_id = [e['exportId'] for e in job['result']][0]

            status = [e['status'] for e in post_hook.run('bulk/v1/leads/export/{0}/enqueue.json'.format(export_id),
                                                         token=self.token).json()['result']][0]
            while status != 'Completed':
                status = [e['status'] for e in get_hook.run('bulk/v1/leads/export/{0}/status.json'.format(export_id),
                                                            token=self.token).json()['result']][0]
                logging.info('Status: ' + str(status))
                sleep(5)

            output = get_hook.run('bulk/v1/leads/export/{0}/file.json'.format(export_id),
                                  token=self.token).text

            output = output.split('\n')
            headers = output.pop(0).split(',')
            del output[0]
            headers = [boa.constrict(header) for header in headers]
            output = [row for row in reader(output)]
            output = [dict(zip(headers, row)) for row in output]
            marketo_schema = schema[self.endpoint]
            field_names = []
            for field in marketo_schema['fields']:
                field_names.append(field['name'])
            logging.info('DIFF: ' + str(set(headers) - set(field_names)))
        else:
            output = self.paginate_data()
            logging.info(len('Output Length: ' + str(output)))

        if len(output) == 0 or output is None:
            logging.info("No records pulled from Marketo.")
            downstream_tasks = context['task'].get_flat_relatives(upstream=False)
            logging.info('Skipping downstream tasks...')
            logging.debug("Downstream task_ids %s", downstream_tasks)

            if downstream_tasks:
                self.skip(context['dag_run'],
                          context['ti'].execution_date,
                          downstream_tasks)

            return True
        else:
            self.outputManager(self.nullify_output(output),
                               self.s3_key,
                               self.s3_bucket,
                               self.output_format)

    def methodMapper(self, endpoint):
        """
        This method maps the desired object to the relevant endpoint.
        """
        mapping = {"auth": "identity/oauth/token",
                   "activities": "rest/v1/activities.json",
                   "activity_types": "rest/v1/activities/types.json",
                   "campaigns": "rest/v1/campaigns.json",
                   "leads_create": "bulk/v1/leads/export/create.json",
                   "lead_description": 'rest/v1/leads/describe.json',
                   "lead_lists": "rest/v1/lists.json",
                   "paging_token": 'rest/v1/activities/pagingtoken.json',
                   "programs": "rest/asset/v1/programs.json",
                   }

        return mapping[endpoint]

    def paginate_data(self, endpoint=None, payload=None):
        if not endpoint:
            endpoint = self.endpoint

        def make_request(http_conn_id,
                         endpoint,
                         payload=None,
                         token=None):

            return (MarketoHook(http_conn_id=http_conn_id)
                    .run(endpoint, payload, token=token)
                    .json())

        final_payload = {}

        for param in self.payload:
            final_payload[param] = self.payload[param]

        if payload:
            for param in payload:
                final_payload[param] = payload[param]

        response = make_request(self.marketo_conn_id,
                                self.methodMapper(endpoint),
                                final_payload,
                                self.token)

        if endpoint == 'paging_token':
            return response['nextPageToken']
        else:
            output = response['result']

            if 'moreResult' in list(response.keys()):
                final_payload['moreResult'] = response['moreResult']
            else:
                final_payload['moreResult'] = False

            while final_payload['moreResult']:
                response = make_request(self.marketo_conn_id,
                                        self.methodMapper(endpoint),
                                        final_payload,
                                        self.token)
                final_payload['nextPageToken']
                if 'result' in (response.keys()):
                    output += response['result']
                    if 'moreResult' in list(response.keys()):
                        final_payload['moreResult'] = response['moreResult']
                        final_payload['nextPageToken'] = response['nextPageToken']
                    else:
                        final_payload['moreResult'] = False
                else:
                    final_payload['moreResult'] = False

            output = [{boa.constrict(k): v for k, v in i.items()} for i in output]
            return output

    def outputManager(self, output, key, bucket, output_format='json'):
        if output_format == 'avro':
            avro_schema = avro.schema.Parse(json.dumps(schema[self.endpoint]))
            writer = DataFileWriter(open("{0}.avro".format(self.endpoint), "wb"),
                                    DatumWriter(),
                                    avro_schema)
            for record in output:
                writer.append(record)

            writer.close()

            output_file = "{0}.avro".format(self.endpoint)

        elif output_format == 'json':
            tmp = NamedTemporaryFile("w")

            for row in output:
                tmp.write(json.dumps(row) + '\n')

            tmp.flush()

            output_file = tmp.name

        s3 = S3Hook(s3_conn_id=self.s3_conn_id)

        s3.load_file(
            filename=output_file,
            key=self.s3_key,
            bucket_name=self.s3_bucket,
            replace=True
        )

    def nullify_output(self, output):
        for record in output:
            for k, v in record.items():
                if v == 'null':
                    record[k] = None
        return output
