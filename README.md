# Plugin - Marketo to S3

This plugin moves data from the [Marketo](http://developers.marketo.com/rest-api/) API to S3 based on the specified object

## Hooks
### MarketoHook
This hook handles the authentication and request to Marketo. This extends the HttpHook.

### S3Hook
[Core Airflow S3Hook](https://pythonhosted.org/airflow/_modules/S3_hook.html) with the standard boto dependency.

## Operators
### MarketoToS3Operator
This operator composes the logic for this plugin. It fetches the Marketo specified object and saves the result in a S3 Bucket, under a specified key, in
njson format. The parameters it can accept include the following.

#### NOTE: The only currently supported + tested output format is json. There are references to avro in this code but support for that format had to be delayed.

`marketo_conn_id`         The Airflow connection id used to store the Marketo credentials.
`marketo_endpoint`        The endpoint to retreive data for. Possible values include:
                            - activities
                            - campaigns
                            - leads
                            - programs
                            - lead_lists
- `start_at`              The starting date parameter. Ignored by all endpoints but leads.
- `end_at`                The ending date parameter. Ignored by all endpoints but leads.
- `payload`               Payload variables -- all are optional
- `output_format`         The output format of the data. Possible values include:
                              - json
                              - avro
                              Defaults to json.
- `s3_conn_id`            The Airflow connection id used to store the S3 credentials.
- `s3_bucket`             The S3 bucket to be used to store the Marketo data.
- `s3_key`                The S3 key to be used to store the Marketo data.
