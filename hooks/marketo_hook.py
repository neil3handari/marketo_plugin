from airflow.hooks.http_hook import HttpHook


class MarketoHook(HttpHook):
    """
    Marketo Hook
    Inherits from the HttpHook to make a request to Marketo.
    Uses basic authentication via a never expiring token that should
    be stored in the 'Login' field in the Airflow Connection panel.

    If retrieving an OAUTH token, this hook expects three parameters:
        - Host - Instance URL issued by Marketo - https://XXX-XXX-XXX.mktorest.com/
        - Extra - Client Id and Client - Both issued by Marketo
            - {"client_id":"XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX",
               "client_secret":"XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"}

    If using OAUTH token directly, this token must be passed in as 'token' value.
    Defaults to GET requests.
    """

    def __init__(self, method='GET', http_conn_id='http_default'):
        self.connection = self.get_connection(http_conn_id)
        self.CLIENT_ID = self.connection.extra_dejson.get('client_id')
        self.CLIENT_SECRET = self.connection.extra_dejson.get('client_secret')
        super().__init__(method, http_conn_id)

    def run(self,
            endpoint,
            data=None,
            headers=None,
            extra_options=None,
            token=None):
        self.endpoint = endpoint

        if endpoint == 'identity/oauth/token':
            data = {"grant_type": "client_credentials",
                    "client_id": "{0}".format(self.CLIENT_ID),
                    "client_secret": "{0}".format(self.CLIENT_SECRET)}
        else:
            headers = {"Authorization": "Bearer {0}".format(token),
                       "Content-Type": "application/json"}
        return super().run(endpoint, data, headers, extra_options)
