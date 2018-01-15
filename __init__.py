from airflow.plugins_manager import AirflowPlugin
from MarketoPlugin.hooks.marketo_hook import MarketoHook
from MarketoPlugin.operators.marketo_to_s3_operator import MarketoToS3Operator


class MarketoPlugin(AirflowPlugin):
    name = "marketo_plugin"
    operators = [MarketoToS3Operator]
    hooks = [MarketoHook]
