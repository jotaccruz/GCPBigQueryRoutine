"""
Copyright 2021 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

# This is a Google Cloud Function which can add the necessary labels to these resources.
# Fine-grained permissions needed are in parentheses.
# Compute Engine VMs - compute.instances.get,compute.instances.setLabels
# GKE Clusters - container.clusters.get,container.clusters.update
# Google Cloud Storage buckets - storage.buckets.get,storage.buckets.update
# Cloud SQL databases - cloudsql.instances.get,cloudsql.instances.update



# Sample deployment command
# gcloud functions deploy Resourcelabeler --runtime python39 --trigger-topic ${TOPIC_NAME} --service-account="${SERVICE_ACCOUNT}" --project ${PROJECT_ID} --retry

from googleapiclient import discovery
from googleapiclient.errors import HttpError
import google.auth
import json
import base64
import re
import os
from google.cloud import bigquery
from google.cloud.bigquery import Dataset
from datetime import datetime


def get_variables_dynamic(request):
    # Get the current date
    current_date = datetime.now()
    # Format the date as MMDDYY
    formatted_date = current_date.strftime("%m%d%y")

    request_json = request.get_json(silent=True)

    variables = {}

    if request_json and 'data' in request_json:
        eventdata = request_json['data']

        if 'source_project' in eventdata:
            variables['source_project'] = eventdata['source_project']
        else:
            variables['source_project'] = "ti-dba-devenv-01"

        if 'source_dataset' in eventdata:
            variables['source_dataset'] = eventdata['source_dataset']
        else:
            variables['source_dataset'] = "teluscsstest"

        if 'source_table' in eventdata:
            variables['source_table'] = eventdata['source_table']
        else:
            variables['source_table'] = "PERSONAL_CALLBACKS"

        if 'target_project' in eventdata:
            variables['target_project'] = eventdata['target_project']
        else:
            variables['target_project'] = "ti-dba-devenv-01"

        if 'target_dataset' in eventdata:
            variables['target_dataset'] = eventdata['target_dataset']
        else:
            variables['target_dataset'] = "teluscsstest"

        if 'target_table' in eventdata:
            variables['target_table'] = eventdata['target_table']
        else:
            variables['target_table'] = "PERSONAL_CALLBACKS" + '_' + formatted_date
    return variables


#{"SourceName":["ti-dba-devenv-01","ti-ca-infrastructure"],"asset_types":[".*.googleapis.com.*Instance"]}
