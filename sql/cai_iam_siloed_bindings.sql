/*************************************************************************************************************************************
Extracts siloed bindings given to users in Pŕojects, Folders and Organization level in a GCP organization.
The query uses the tables created by the Cloud Asset Inevntory export.
https://cloud.google.com/asset-inventory/docs/export-bigquery


Author: Josue Velazquez (josuegen@google.com)
***************************************************************************************************************************************/

SELECT
  asset_type,
  iam.name,
  res.project_id,
  member,
  role
FROM
  (
    SELECT *,binding
    FROM `<project>.<dataset>.iam_policy`, -- TODO: Developer to set the project and dataset ID where the export was saved
    UNNEST(iam_policy.bindings) binding
    WHERE asset_type IN ('cloudresourcemanager.googleapis.com/Project', 'cloudresourcemanager.googleapis.com/Folder')
    AND DATE(requestTime) = CURRENT_DATE()
  ) iam,
UNNEST(iam.binding.members) member
JOIN (
  SELECT
    name,
    COALESCE(
      JSON_VALUE(PARSE_JSON(resource.resource.data), '$.projectId'),
      JSON_VALUE(PARSE_JSON(resource.resource.data), '$.displayName')
    ) as project_id
  FROM <project>.<dataset>.resource -- TODO: Developer to set the project and dataset ID where the export was saved
  WHERE asset_type IN ('cloudresourcemanager.googleapis.com/Project', 'cloudresourcemanager.googleapis.com/Folder')
  AND DATE(requestTime) = CURRENT_DATE()
) res
ON iam.name = res.name
WHERE member LIKE 'user:%'
