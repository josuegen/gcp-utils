# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import requests

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, concat_ws, col, lit, concat, coalesce, from_json
from pyspark.sql.types import StringType, StructType, StructField

from google.auth import default
import google.auth.transport.requests

df = spark.createDataFrame([("1010 Fourth St Newton, CA, 90213",)], ["full_address"])

@udf(returnType=StringType())
def get_geocode_data(address, token):
    """
    Get geocode data from the Google Maps API.
    """
    if address is None:
        return None
    
    output_dict = {}
    url = "https://addressvalidation.googleapis.com/v1:validateAddress"
    address_dict = """{{"address": {{"addressLines": ["{}"]}}}}""".format(address)
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    response = requests.post(url=url, data=address_dict, headers=headers).json()
    
    formatted_address = str(response.get("result").get("address").get("formatted_address"))
    latitude = str(response.get("result").get("geocode").get("location").get("latitude"))
    longitude = str(response.get("result").get("geocode").get("location").get("longitude"))
    output_dict.update(formatted_address=formatted_address)
    output_dict.update(latitude=latitude)
    output_dict.update(longitude=longitude)
    
    str_response = json.dumps(output_dict)
    
    return str_response

json_schema = StructType(
    [
        StructField('formatted_address', StringType(), True),
        StructField('latitude',StringType(), True),
        StructField('longitude', StringType(), True)
    ]
)

# Gets default credentials
credentials, project = google.auth.default(
    scopes=["https://www.googleapis.com/auth/cloud-platform"]
)
request = google.auth.transport.requests.Request()
credentials.refresh(request)
access_token = credentials.token

df = df.withColumn(
    "nested_validated_address",
    get_geocode_data(df["full_address"], lit(access_token))
)