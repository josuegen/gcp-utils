"""
Cloud Asset Inventory Export

Cloud Function to export the Clouid Asset Inventory to BigQuery
https://cloud.google.com/asset-inventory/docs/export-bigquery

Author: josuegen@google.com
"""

import functions_framework

@functions_framework.http
def hello_http(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """
    from google.cloud import asset_v1

    
    request_json = request.get_json(silent=True)
    request_args = request.args

    contents = ['IAM_POLICY', 'RESOURCE'] # TODO: Developer can adjust the content type to export: https://cloud.google.com/asset-inventory/docs/asset-inventory-overview#content_types

    project_id = '<project-id>' # TODO: Developer to set the project ID where the data wil be stored
    dataset = f'projects/{project_id}/datasets/ds_cloud_asset_inventory' # TODO: Developer to set the dataset ID where the data wil be stored

    client = asset_v1.AssetServiceClient(
        client_options={'quota_project_id': project_id}
    )
    parent = "organizations/<organization-id>" # TODO: Developer to set the organization ID


    for content in contents:
        table = content.lower()
        content_type = content

        output_config = asset_v1.OutputConfig()
        output_config.bigquery_destination.dataset = dataset
        output_config.bigquery_destination.table = table
        output_config.bigquery_destination.partition_spec = {'partition_key': 'REQUEST_TIME'} # TODO: Developer can choose the partition key: https://cloud.google.com/asset-inventory/docs/reference/rpc/google.cloud.asset.v1#partitionkey

        response = client.export_assets(
            request={
                "parent": parent,
                "content_type": content_type,
                "output_config": output_config,
            }
        )

        print(response.result())
