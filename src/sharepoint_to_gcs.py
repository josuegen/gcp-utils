import json
import requests as r
import functools
from google.cloud import storage
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class SharePointUtility(object):
    class _Decorators(object):
        @classmethod
        def digest_code_decorator(class_obj, decorated_token_function):
            @functools.wraps(decorated_token_function)
            def wrapper_obtain_digest(self_obj):
                tk_sharepoint = decorated_token_function(self_obj)
                print("OAuth token : {}".format(tk_sharepoint[:5:1] + "..." + tk_sharepoint[-5:]))
                print(tk_sharepoint)
                digest_tk = self_obj.obtain_digest(tk_sharepoint)
                self_obj.digest_tk = digest_tk
                print("Digest Code: {}".format(digest_tk))
                return digest_tk

            return wrapper_obtain_digest

    def __init__(self, application_details, gcs_details, file_param, tcreds, *args, **kwargs):
        self.application_details = application_details
        self.gcs_details = gcs_details
        self.file_param = file_param
        self.oauth_tk = ""
        self.digest_tk = ""
        self.initialize_tokens()
        self.tcreds = tcreds

    @_Decorators.digest_code_decorator
    def initialize_tokens(self):
        return self.oauth_token()

    # Generate the token from client id and secret
    def oauth_token(self):
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        payload = {
            "grant_type": "client_credentials",
            "client_id": self.application_details["client_id"],
            "client_secret": self.application_details["client_secret"],
            "resource": "00000003-0000-0ff1-ce00-000000000000/{}@{}".format(
                self.application_details["sharepoint_site_domain"],
                self.application_details["tenant_id"],
            ),
        }
        oauth_token_request_url = (
            "https://accounts.accesscontrol.windows.net/{}/tokens/OAuth/2".format(
                self.application_details["tenant_id"]
            )
        )
        response_raw = r.post(oauth_token_request_url, headers=headers, data=payload)
        oauth_token_for_crud = response_raw.json()["access_token"]
        self.oauth_tk = oauth_token_for_crud
        return oauth_token_for_crud

    # decorator to test the generated token
    def obtain_digest(self, oauth_token):
        context_info_url = "https://{}/sites/{}/_api/contextinfo".format(
            self.application_details["sharepoint_site_domain"],
            self.application_details["app_sub_site"],
        )
        headers = {
            "Authorization": "Bearer {}".format(oauth_token),
            "Content-Type": "application/json;odata=verbose",
            "Accept": "application/json;odata=verbose",
        }
        response_raw = r.post(context_info_url, headers=headers)
        print(response_raw.status_code)
        return response_raw.json()["d"]["GetContextWebInformation"]["FormDigestValue"]

    # Download the files from sharepoint to GCS. If the file list is empty, downloads all the files from given folder
    def download(self, relative=True):
        if len(self.file_param["file_name"]) > 0:
            for file in self.file_param["file_name"]:
                try:
                    file_relative_api_url = "https://{}/sites/{}/_api/Web/GetFileByServerRelativePath(decodedurl='{}/{}')/$value"
                    framed_download_url = file_relative_api_url.format(
                        self.application_details["sharepoint_site_domain"],
                        self.application_details["app_sub_site"],
                        self.application_details["folder_relative_path"],
                        file,
                    )
                    headers = {
                        "Authorization": "Bearer {}".format(self.oauth_tk),
                        "Accept": "application/json;odata=verbose",
                    }
                    s = r.Session()
                    # couple of retries with the 5 sec interval when failure
                    retries = Retry(total=2, backoff_factor=5)
                    s.mount("https://", HTTPAdapter(max_retries=retries))
                    response_raw = s.get(framed_download_url, headers=headers)
                    if str(response_raw.status_code) == "200":
                        gcs_upload = self.gcs_storage(response_raw.content, file)
                        if gcs_upload:
                            print(
                                "File {} uploaded into GCS with the extension {}".format(
                                    file, self.gcs_details["folder"]
                                )
                            )
                        else:
                            print("Failed to upload the file {} into GCS".format(file))
                    else:
                        print(
                            "File {} *NOT* Downloaded in to gcs {}".format(
                                file, response_raw.status_code
                            )
                        )
                except Exception as e:
                    print(
                        "While downloding the file {} the exception {} is occured".format(file, e)
                    )
                    pass
        else:
            print("Given files are not avaialble in sharepoint to download")

    # Get the file list from the given sharepoint folder
    def list_files(self, relative=True):
        try:
            if len(self.file_param["file_name"]) > 0:
                file_relative_api_url = (
                    "https://{}/sites/{}/_api/web/GetFolderByServerRelativeUrl('{}')/Files"
                )
                framed_download_url = file_relative_api_url.format(
                    self.application_details["sharepoint_site_domain"],
                    self.application_details["app_sub_site"],
                    self.application_details["folder_relative_path"],
                )
            elif (
                len(self.file_param["start_datetime"]) > 0
                and len(self.file_param["end_datetime"]) > 0
            ):
                file_relative_api_url = "https://{}/sites/{}/_api/web/GetFolderByServerRelativeUrl('{}')/Files?$filter=TimeLastModified gt datetime'{}' and TimeLastModified lt datetime'{}'"
                framed_download_url = file_relative_api_url.format(
                    self.application_details["sharepoint_site_domain"],
                    self.application_details["app_sub_site"],
                    self.application_details["folder_relative_path"],
                    self.file_param["start_datetime"],
                    self.file_param["end_datetime"],
                )
            elif (
                len(self.file_param["start_datetime"]) > 0
                and len(self.file_param["end_datetime"]) == 0
            ):
                file_relative_api_url = "https://{}/sites/{}/_api/web/GetFolderByServerRelativeUrl('{}')/Files?$filter=TimeLastModified gt datetime'{}'"
                framed_download_url = file_relative_api_url.format(
                    self.application_details["sharepoint_site_domain"],
                    self.application_details["app_sub_site"],
                    self.application_details["folder_relative_path"],
                    self.file_param["start_datetime"],
                )
            headers = {
                "Authorization": "Bearer {}".format(self.oauth_tk),
                "Accept": "application/json;odata=verbose",
            }

            s = r.Session()
            # couple of retries with the 5 sec interval when failure
            retries = Retry(total=2, backoff_factor=5)
            s.mount("https://", HTTPAdapter(max_retries=retries))

            response_raw = s.get(framed_download_url, headers=headers)
            print(framed_download_url)
            print(response_raw.status_code)
            print(response_raw.content)
            if str(response_raw.status_code) == "200":
                file_list = json.loads(response_raw.content)
                file_list = file_list["d"]["results"]
                file_name = []
                for f in file_list:
                    file_name.append(f["Name"])
                print(file_name)
                return file_name
            else:
                print(
                    "*NOT* able to list the files in Sharepoint Path {} with {}".format(
                        self.application_details["folder_relative_path"], response_raw.status_code
                    )
                )
        except Exception as e:
            print(
                "Exception occured while retriving the list of files from given sharepoint folder {}".format(
                    e
                )
            )

    # *** delete the given list of files, if the file list is empty the entire files in the folder will be deleted ***
    def delete_files(self):
        if len(self.file_param["file_name"]) > 0:
            for file in self.file_param["file_name"]:
                try:
                    file_relative_api_url = "https://{}/sites/{}/_api/Web/GetFileByServerRelativePath(decodedurl='{}/{}')"
                    framed_download_url = file_relative_api_url.format(
                        self.application_details["sharepoint_site_domain"],
                        self.application_details["app_sub_site"],
                        self.application_details["folder_relative_path"],
                        file,
                    )
                    headers = {
                        "Authorization": "Bearer {}".format(self.oauth_tk),
                        "Accept": "application/json;odata=verbose",
                    }
                    s = r.Session()
                    # couple of retries with the 5 sec interval when failure
                    retries = Retry(total=2, backoff_factor=5)
                    s.mount("https://", HTTPAdapter(max_retries=retries))
                    response_raw = s.delete(framed_download_url, headers=headers)
                    print(response_raw.status_code)
                    if str(response_raw.status_code) == "200":
                        print("File {} deleted".format(file))
                except Exception as e:
                    print("While deleting the file {} the exception {} is occured".format(file, e))
                    pass
        else:
            print("Given files are not avaialble in sharepoint to delete")

    #  File upload to GCS
    def gcs_storage(self, raw_data, file_name):
        try:
            # pass tcreds as None if impersonation not required
            if self.tcreds is not None:
                client = storage.Client(
                    project=self.gcs_details["project_id"], credentials=self.tcreds
                )
            else:
                client = storage.Client(project=self.gcs_details["project_id"])
            bucket = client.get_bucket(self.gcs_details["bucket"])
            blob = bucket.blob("{}/{}".format(self.gcs_details["folder"], file_name))
            blob.upload_from_string(raw_data)
            return True
        except Exception as e:
            print("gcs upload failed for the file {} with the exception {}".format(file_name, e))
            return False
