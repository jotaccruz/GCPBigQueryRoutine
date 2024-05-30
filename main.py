import base64
from google.cloud import bigquery
import logging
import modules
from modules import *
import json
from flask import escape
import functions_framework


logger = logging.getLogger()


# [START functions_labels_http]
@functions_framework.http
def teluscss_http(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """
    variables = get_variables_dynamic(request)

    teluscss_personal_callbacks_delete(variables)

    return f"Ready {escape(variables['source_project'] +'.'+ variables['source_dataset'] +'.'+ variables['source_table'])} was purged successfully!\n"

# [END functions_labels_http]


def teluscss_personal_callbacks_delete(variables):

    client = bigquery.Client()

    # Define source and destination datasets
    source_table_id = variables['source_project'] +'.'+ variables['source_dataset'] +'.'+ variables['source_table']
    destination_table_id = variables['target_project'] +'.'+ variables['target_dataset'] +'.'+ variables['target_table']

    # Perform the copy
    job = client.copy_table(source_table_id, destination_table_id)
    job.result()  # Wait for the job to complete

    print(f"Copied {source_table_id} to {destination_table_id}")

    query = 'DELETE FROM ' + source_table_id + ' WHERE objectId in ( SELECT objectId from ( SELECT objectId, deletedAt, ROW_NUMBER() OVER ( PARTITION BY objectId ORDER BY createdAt DESC ) row_num FROM ' + source_table_id + ' WHERE DATE(_PARTITIONTIME) < DATE_SUB(DATE (CURRENT_DATE()), INTERVAL 3 DAY) ) WHERE row_num = 1 AND deletedAt > 0 )'
    #logger.warning(query)

    # Perform a query.
    QUERY = (
        query
        )
    query_job = client.query(QUERY)  # API request
    assets1 = query_job.result()  # Waits for query to finish

    print(f"Purge complete over: {source_table_id}")
