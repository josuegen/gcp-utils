## Sharepoint to GCS

This util is got three different functions
1. list the files in the given folder using (def list_files). If date range is passed, it will list only the files which are in the range,
otherwise list all the files in the folder.
2. download the files (def download) to cloud storage. Pass the list of files to be downloaded
3. delete files using (def delete_files). It generates a token using the sharepoint connection details and use it while executing the functions

The object structure to instantiate the class
application_details['tenant_id'] = <tenant id for the sharepoint access>
application_details['client_id'] = <sharepoint user id>
application_details['client_secret'] = <sharepoint password>
application_details['sharepoint_site_domain'] = xxx.sharepoint.com
application_details['app_sub_site'] = <sub_site>
application_details['folder_relative_path'] = <folder Name which navigates from site domain>
the following one of these are mandatory: if datetime range is given, the list function returns the files which are in the range or lists all files.
file_param['file_name'] = <file names in a list>
file_param['start_datetime'] = <start datetime to be given> format '2020-08-01T09:59:32'. Leave it empty string if not used
file_param['end_datetime'] = <end datetime to be given> format '2020-08-01T09:59:32'. Leave it empty string if not used
