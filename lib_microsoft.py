from dataclasses import dataclass
from datetime import datetime
import requests
import pandas as pd
import re
import os
import zipfile
import json
import inspect
from dotenv import load_dotenv
from io import BytesIO
import base64

load_dotenv()

@dataclass
class microsoft_sharepoint:
    client_id: str = os.getenv('client_id')
    client_secret: str = os.getenv('client_secret')
    tenant_id: str = os.getenv('tenant_id')
    site_id: str = os.getenv('site_id')
    list_id: str = os.getenv('list_id')
    base_url: str = "https://graph.microsoft.com/v1.0/sites"

    def get_token(self, **kwargs):
        client_id= self.client_id 
        client_secret = self.client_secret
        if kwargs.get('group') == 'P-Doo Clearing Global':
            client_id = os.getenv('pclear_client_id')
            client_secret = os.getenv('pclear_client_secret')
        authUrl = f'https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token'
        body = {
            'grant_type':'client_credentials',
            'scope':'https://graph.microsoft.com/.default',
            'client_id':client_id,
            'client_secret':client_secret
        }
        response = requests.post(authUrl, data=body)
        result = response.json()
        access_token = result['access_token']
        return {
            'Authorization':f'Bearer {access_token}'
        }
    
    def update_log(self, **kwargs):
        kwargs['fileName'] = 'Microsoft Graph API log.xlsx' ##Default
        kwargs['parentFolderName'] = 'Shared Document/Reporting and Data Management/Connector Log'
        data = [{
            'execution_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'username':os.getlogin(),
            'function_used':kwargs.get('function_used'),
            'kwargs':str(kwargs.get('kwargs')),
            'response_code':kwargs.get('response_code')
        }]
        new_record = pd.DataFrame(data)
        df = self.read_item(**kwargs)
        df['execution_at'] = df['execution_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
        latest_df = pd.concat([df, new_record])
        return self.update_same_file(latest_df, **kwargs)
    
    def check_group(self, **kwargs):
        site_id = self.site_id
        list_id = self.list_id
        if kwargs.get('group') == 'D-Reporting & Data Management DP':
            site_id = os.getenv('d_site_id')
            list_id = os.getenv('d_list_id')
        elif kwargs.get('group') == 'P-Data Management DF':
            site_id = os.getenv('df_site_id')
            list_id = os.getenv('df_list_id')
        elif kwargs.get('group') == 'D-Broker Back Office DF - Broker Back Office DF AU':
            site_id = os.getenv('dbroker_site_id')
            list_id = os.getenv('dbroker_list_id')
        elif kwargs.get('group') == 'P-Doo Clearing Global':
            site_id = os.getenv('pclear_site_id')
            list_id = os.getenv('pclear_list_id')
        return site_id, list_id

    def list_files(self, **kwargs):
        site_id, list_id = self.check_group(**kwargs)
        if not kwargs.get('parentFolderID') and not kwargs.get('parentFolderName'):
            return False, 'There is no parentFolderID and parentFolderName to search!'
        header = self.get_token(**kwargs)
        if kwargs.get('parentFolderID'):
            requestURL = f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items/{kwargs.get("parentFolderID")}/driveItem/children'
        if kwargs.get('parentFolderName'):
            requestURL = f'https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{kwargs.get("parentFolderName")}:/children'
        result = []
        response = requests.get(requestURL, headers=header)
        resultJson = response.json()
        # Determine whether to list only files or only folders
        list_files_only = kwargs.get('files_only', False)
        list_folders_only = kwargs.get('folders_only', False)

        while True:
            if resultJson.get('value'):
                for item in resultJson.get('value'):
                    is_folder = 'folder' in item  # True if it's a folder
                    if list_files_only and is_folder:
                        continue  # Skip folders if only files are needed
                    if list_folders_only and not is_folder:
                        continue  # Skip files if only folders are needed
                    result.append(item.get('name'))
            if not '@odata.nextLink' in resultJson:
                break
            response = requests.get(resultJson['@odata.nextLink'], headers=header)
            resultJson = response.json()
        log_kwargs = {
            'function_used':inspect.currentframe().f_code.co_name,
            'kwargs': kwargs,
            'response_code':response.status_code,
        }
        self.update_log(**log_kwargs)
        if kwargs.get('returnTuple', False):
            return tuple(result)
        return result
    
    def search_itemID(self, **kwargs):
        site_id, list_id = self.check_group(**kwargs)
        if not kwargs.get('fileName') and not kwargs.get('parentFolderName') and not kwargs.get('fileURL'):
            return False, 'There is no fileName, parentFolderName or fileURL to search!'
        header = self.get_token()
        fileName = kwargs.get('fileName')
        parentFolderName = kwargs.get('parentFolderName')
        if kwargs.get('fileURL'):
            parentFolderName, fileName = kwargs.get('fileURL').rsplit('/', 1)
        fileUrl = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items?$select=lastModifiedDateTime,id,contentType&expand=fields(select=FileLeafRef,id)&filter=fields/FileLeafRef eq '{fileName}'"
        header['Prefer']='HonorNonIndexedQueriesWarningMayFailRandomly'
        response = requests.get(fileUrl, headers=header)
        result = response.json()
        if result['value'] == []:
            return False, 'There is no file exist!'
        resultID = pd.DataFrame(result['value'])
        if not parentFolderName and len(result['value']) > 1:
            return False, 'There are multiple same name files exist! Please insert the parentFolderName to get the correct file!'
        crossCheck = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{parentFolderName}/{fileName}:/"
        checkID = requests.get(crossCheck, headers=header)
        checkID = re.search(r'\{(.+?)\}', checkID.json().get('eTag')).group(1).lower()
        resultID = resultID.loc[resultID['@odata.etag'].str.contains(checkID)]
        return(resultID['fields'][0].get('id'))
    
    def search_itemName(self, **kwargs):
        site_id, list_id = self.check_group(**kwargs)
        if not kwargs.get('fileID'):
            return False, 'There is no fileID for searching'
        header = self.get_token()
        fileUrl = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items/{kwargs.get('fileID')}"
        response = requests.get(fileUrl, headers=header)
        result = response.json()
        try:
            filename = result['fields']['FileLeafRef']
            return filename
        except Exception as e:
            return False, str(e)  
        
    def download_url(self, **kwargs):
        site_id, list_id = self.check_group(**kwargs)
        header = self.get_token()
        fileName = kwargs.get('fileName')
        parentFolderName = kwargs.get('parentFolderName')
        crossCheck = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{parentFolderName}/{fileName}:/"
        response = requests.get(crossCheck, headers=header)
        if response.status_code == 200:
            result = response.json()
            return result['@microsoft.graph.downloadUrl']

    def download_item(self, **kwargs):
        download_url = self.download_url(**kwargs)
        fileName = kwargs.get('fileName')
        if kwargs.get('fileURL'):
            parentFolderName, fileName = kwargs.get('fileURL').rsplit('/', 1)
        if kwargs.get('folderName'):
            fileName = f"{kwargs.get('folderName')}/{fileName}"
        response = requests.get(download_url, stream=True)
        if response.status_code == 200:
            with open(fileName, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
        log_kwargs = {
            'function_used':inspect.currentframe().f_code.co_name,
            'kwargs': kwargs,
            'response_code':response.status_code,
        }
        self.update_log(**log_kwargs)
        return True 

    def delete_item(self, **kwargs):
        site_id, list_id = self.check_group(**kwargs)
        header = self.get_token()
        if kwargs.get('fileID'):
            itemID = kwargs.get('fileID')
        else:
            itemID = self.search_itemID(**kwargs)
        if not itemID[0]:
            return itemID
        requests_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items/{itemID}"
        try:
            response = requests.delete(requests_url, headers=header)
            log_kwargs = {
                'function_used':inspect.currentframe().f_code.co_name,
                'kwargs': kwargs,
                'response_code':response.status_code,
            }
            self.update_log(**log_kwargs)
            if response.status_code == 204:
                return True
        except Exception as e:
            return False, str(e)   

    def upload_item(self, **kwargs):
        site_id, list_id = self.check_group(**kwargs)
        if not kwargs.get('fileName') and not kwargs.get('parentFolderName') and not kwargs.get('fileURL'):
            return False, 'There is no fileName, parentFolderName or fileURL to search!'
        header = self.get_token()
        fileName = kwargs.get('fileName')
        parentFolderName = kwargs.get('parentFolderName')
        fileLocated = kwargs.get('fileLocated')
        if kwargs.get('fileURL'):
            parentFolderName, fileName = kwargs.get('fileURL').rsplit('/', 1)
        with open(f'.{fileLocated}/{fileName}', 'rb') as f:
            data = f.read()
        requests_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{parentFolderName}/{fileName}:/content"
        response = requests.put(requests_url, headers=header, data=data)
        log_kwargs = {
            'function_used':inspect.currentframe().f_code.co_name,
            'kwargs': kwargs,
            'response_code':response.status_code,
        }
        self.update_log(**log_kwargs)
        if response.status_code == 201 or response.status_code == 200:
            return True
        return False
    
    def update_same_file(self, df = pd.DataFrame(), **kwargs):
        """
        group: str [Default: P-Data Management DP],
        df: dataframe [Default: empty],
        fileName: str [Default: empty],
        parentFolderName: str [Default: empty],
        fileURL: str [Default: empty]
        """
        site_id, list_id = self.check_group(**kwargs)
        header = self.get_token()
        if df.empty:
            return 'Missing Dataframe!'
        if not kwargs.get('fileName') and not kwargs.get('parentFolderName') and not kwargs.get('fileURL'):
            return False, 'There is no fileName, parentFolderName or fileURL to search!'
        data = df.values.tolist()
        num_rows = len(data)
        num_columns = len(df.columns)
        end_column_letter = chr(ord('A') + num_columns - 1)
        range_address = f"A2:{end_column_letter}{num_rows + 1}"
        update_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{kwargs.get('parentFolderName')}/{kwargs.get('fileName')}:/workbook/worksheets('Sheet1')/range(address='{range_address}')"
        clear_url = update_url+"/clear"
        clear_response = requests.post(clear_url, headers=header)
        if clear_response.status_code != 204: 
            return False, f"Failed to clear the existing content. Status code: {clear_response.status_code}, Message: {clear_response.text}"
        body = {
            "values": data
        }
        update_response = requests.patch(update_url, headers={**header, "Content-Type": "application/json"}, data=json.dumps(body))
        return update_response.status_code
    
    def read_item(self, **kwargs):
        site_id, list_id = self.check_group(**kwargs)
        if not kwargs.get('fileName') and not kwargs.get('parentFolderName') and not kwargs.get('fileURL'):
            return False, 'There is no fileName, parentFolderName or fileURL to search!'
        header = self.get_token(**kwargs)
        fileName = kwargs.get('fileName')
        parentFolderName = kwargs.get('parentFolderName')
        if kwargs.get('fileURL'):
            parentFolderName, fileName = kwargs.get('fileURL').rsplit('/', 1)
        requests_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{parentFolderName}/{fileName}:/content"
        response = requests.get(requests_url, headers=header)
        file_content = BytesIO(response.content)
        if fileName.split(".")[-1] == 'csv':   
            return pd.read_csv(file_content)
        elif fileName.split(".")[-1] == 'py':
            return self.download_item(**kwargs)
        elif fileName.split(".")[-1] == 'xlsb':
            return pd.read_excel(file_content, sheet_name=kwargs.get('sheet_name','Sheet1'), engine='pyxlsb')
        elif fileName.split(".")[-1] == 'xlsx':
            return pd.read_excel(file_content, sheet_name=kwargs.get('sheet_name','Sheet1'), engine='openpyxl')
        elif fileName.split(".")[-1] == 'parquet':
            return pd.read_parquet(file_content)
        elif fileName.split(".")[-1] == 'zip':
            with zipfile.ZipFile(file_content, 'r') as z:
                with z.open(z.namelist()[0]) as f:
                    return pd.read_csv(f)
        elif fileName.split(".")[-1] == 'txt':
            file_content.seek(0) 
            return pd.read_csv(file_content, sep=kwargs.get('separator',';'), skiprows=kwargs.get('skip_row',1))
        return pd.read_excel(file_content, sheet_name=kwargs.get('sheet_name','Sheet1'))
    
    def show_all_sharepoint_list(self, **kwargs):
        site_id, list_id = self.check_group(**kwargs)
        header = self.get_token()
        requests_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists"
        response = requests.get(requests_url, headers=header).json()
        if response.get('value',False):
            result = pd.DataFrame(response.get('value'))
            return result[['name','id','displayName']]
        return pd.DataFrame()
    
    def read_sharepoint_list(self, **kwargs):
        site_id, list_id = self.check_group(**kwargs)
        df = self.show_all_sharepoint_list(**kwargs)
        if not kwargs.get('listName',False):
            return "No Specific listName!"
        listId = df.loc[df['displayName']==kwargs.get('listName')]['id']
        if len(listId.values) == 0:
            return "listName not exists!"
        listId = listId.values[0]
        header = self.get_token()
        requests_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{listId}/items?expand=fields"
        response = requests.get(requests_url, headers=header).json()
        if response.get('value',False):
            result = pd.DataFrame(response.get('value'))
            final_df = result['fields'].apply(pd.Series)
            return final_df
        return pd.DataFrame()
    
    def get_group_details(self):
        header = self.get_token()
        requests_url = f"https://graph.microsoft.com/v1.0/groups?$filter=mailEnabled eq false&securityEnabled eq true"
        result = []
        response = requests.get(requests_url, headers=header)
        resultJson = response.json()
        while True:
            if resultJson.get('value') != []:
                result += resultJson.get('value')
            if not '@odata.nextLink' in resultJson:
                break
            response = requests.get(resultJson['@odata.nextLink'], headers=header)
            resultJson = response.json()
        if result != []:
            return pd.DataFrame(result)
        return result
    
    def get_groupmember_details(self, **kwargs):
        header = self.get_token()
        groupDf = self.get_group_details()
        groupName = kwargs.get('group_name',False)
        if not groupName:
            return pd.DataFrame()
        if type(kwargs.get('group_name')) != list:
            groupName = [groupName]
        groupDf = groupDf[groupDf['displayName'].isin(groupName)]
        finalDf = []
        if not groupDf.empty:
            groupIds = list(set(groupDf['id']))
            for groupId in groupIds:
                result = []
                requests_url = f"https://graph.microsoft.com/v1.0/groups/{groupId}/members"
                response = requests.get(requests_url, headers=header)
                resultJson = response.json()
                if resultJson.get('value') != []:
                    result += resultJson.get('value')
                while '@odata.nextLink' in resultJson:
                    response = requests.get(resultJson['@odata.nextLink'], headers=header)
                    resultJson = response.json()
                    result += resultJson.get('value')
                groupResult = pd.DataFrame(result)
                groupResult['groupName'] = groupDf[groupDf['id']==groupId]['displayName'].values[0]
                finalDf.append(groupResult)
            finalResult = pd.concat(finalDf)
            if not finalResult.empty:
                return finalResult
        return pd.DataFrame()
    
    def add_member_into_group(self, **kwargs):
        header = self.get_token()
        header['Content-Type'] = 'application/json'
        groupDf = self.get_group_details()
        groupName = kwargs.get('group_name',False)
        if not groupName:
            return 'No Group Name Provided!'
        groupDf = groupDf[groupDf['displayName']==groupName]
        if groupDf.empty:
            return 'No Specific Group!'
        groupId = groupDf['id'].values[0]
        email = kwargs.get('email', False)
        if not email:
            return 'No Email Provided!'
        if type(email) != list:
            email = [email]
        email = [text.lower() for text in email]
        requests_url = f"https://graph.microsoft.com/v1.0/groups/{groupId}/members/$ref"
        userDf = self.read_item(**{'fileURL':'Shared Document/All Employee/All Staff.csv'})
        userDf['email'] = userDf['email'].str.lower()
        resultDf = userDf.loc[userDf['email'].isin(email)]
        if resultDf.empty:
            return False, 'User not found!'
        users_id = list(set(resultDf['object_id'])) 
        for user in users_id:
            payload = json.dumps({
                "@odata.id": f"https://graph.microsoft.com/v1.0/directoryObjects/{user}"
            })
            response = requests.request("POST", requests_url, headers=header, data=payload)
        log_kwargs = {
            'function_used':inspect.currentframe().f_code.co_name,
            'kwargs': kwargs,
            'response_code':response.status_code,
        }
        self.update_log(**log_kwargs)
        return response.status_code
    
    def remove_member_from_group(self, **kwargs):
        header = self.get_token()
        groupDf = self.get_group_details()
        groupName = kwargs.get('group_name',False)
        if not groupName:
            return 'No Group Name Provided!'
        groupDf = groupDf[groupDf['displayName']==groupName]
        if groupDf.empty:
            return 'No Specific Group!'
        groupId = groupDf['id'].values[0]
        email = kwargs.get('email',False)
        if not email:
            return 'No Email Provided!'
        if type(email) != list:
            email = [email]
        email = [text.lower() for text in email]
        userDf = self.read_item(**{'fileURL':'Shared Document/All Employee/All Staff.csv'})
        userDf['email'] = userDf['email'].str.lower()
        resultDf = userDf.loc[userDf['email'].isin(email)]
        if resultDf.empty:
            return False, 'User not found!'
        users_id = list(set(resultDf['object_id'])) 
        for user in users_id:
            requests_url = f"https://graph.microsoft.com/v1.0/groups/{groupId}/members/{user}/$ref"
            payload = {}
            response = requests.request("DELETE", requests_url, headers=header, data=payload)
        log_kwargs = {
            'function_used':inspect.currentframe().f_code.co_name,
            'kwargs': kwargs,
            'response_code':response.status_code,
        }
        self.update_log(**log_kwargs)
        return response.status_code

    def get_general_details_by_id(self, sharepoint_id: int):
        header = self.get_token()
        requests_url = f"{self.base_url}/{self.site_id}/lists/{self.list_id}/items/{sharepoint_id}/driveItem"
        response = requests.get(requests_url, headers=header)
        response_json = response.json()
        response_json["file_path"] = (
            f'{response_json["parentReference"]["path"].split("root:/")[1]}/{response_json["name"]}'
        )
        return response_json

    def get_all_file_details_in_folder(
        self, sharepoint_folder_id: int = None, sharepoint_folder_path: str = None
    ):
        header = self.get_token()
        if sharepoint_folder_id:
            request_url = f"{self.base_url}/{self.site_id}/lists/{self.list_id}/items/{sharepoint_folder_id}/driveItem/children"
        elif sharepoint_folder_path:
            request_url = f"{self.base_url}/{self.site_id}/drive/root:/{sharepoint_folder_path}:/children"
        else:
            raise ValueError("Neither Folder ID or Folder Path Passed In!")
        result = []
        response = requests.get(request_url, headers=header)
        response_json = response.json()
        if response_json.get("value"):
            result = response_json.get("value")
        result_df = pd.DataFrame(result)
        return result_df

    def resultjson(self, **kwargs):
        site_id, list_id = self.check_group(**kwargs)
        if not kwargs.get('parentFolderID') and not kwargs.get('parentFolderName'):
            return False, 'There is no parentFolderID and parentFolderName to search!'
        header = self.get_token()
        if kwargs.get('parentFolderID'):
            requestURL = f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items/{kwargs.get("parentFolderID")}/driveItem/children'
        if kwargs.get('parentFolderName'):
            requestURL = f'https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{kwargs.get("parentFolderName")}:/children'
        result = []
        response = requests.get(requestURL, headers=header)
        resultJson = response.json()
        return resultJson
    
    def get_file_path(
        self,
        sharepoint_folder_id: int,
        sharepoint_folder_path=None,
        file_path_list=None,
        layer: int = 1
    ):
        if file_path_list is None:
            file_path_list = []

        main_path = sharepoint_folder_path or self.get_general_details_by_id(sharepoint_folder_id)['file_path']

        # Process files in the current folder
        all_files = self.list_files(parentFolderName=main_path, files_only=True)
        file_path_list.extend(f"{main_path}/{file_name}" for file_name in all_files)

        # Stop recursion if layer limit is reached
        if layer == 1:
            return file_path_list

        # Process subfolders if deeper layers are needed
        folder_list = self.list_files(parentFolderName=main_path, folders_only=True)
        for subfolder in folder_list:
            subfolder_path = f"{main_path}/{subfolder}"
            self.get_file_path( sharepoint_folder_id, subfolder_path, file_path_list, layer - 1)
    
        return file_path_list

    def search_item_details(self, **kwargs):
        """
        Search file properties in specific folder. 

        This method retrieves file properties such as item id or download url in sharepoint.

        Args:
            kwargs: Arbitrary keyword arguments.:
                - group (str, optional): The group channel name (default is P-Data Management DP).
                - fileName (str, optional): The name of the file. Required if `filePath` or `fileURL` is not provided.
                - parentFolderName (str, optional): The name of the parent folder. Required if `filePath` or `fileURL` is not provided.
                - filePath (str, optional): The file path of the file. Required if `fileName` and `parentFolderName` is not provided or `fileURL` is not provided.
                - fileURL (str, optional): The file shared url of the file. Required if `fileName` and `parentFolderName` is not provided or `filePath` is not provided.
                - get_download_url (bool, optional): Request return download url (default is False)
                - get_filename (bool, optional): Request return filename (default is False)
                
        Returns:
            str: return file ID, or a download url if `get_download_url` is set to True, or a file name if `get_filename` is set to True
        """
        site_id, list_id = self.check_group(**kwargs)
        if not kwargs.get('fileName') and not kwargs.get('parentFolderName') and not kwargs.get('filePath') and not kwargs.get('fileURL'):
            return False, 'There is no fileName and parentFolderName or filePath or fileURL to search!'
        header = self.get_token(**kwargs)
        fileName = kwargs.get('fileName','')
        parentFolderName = kwargs.get('parentFolderName','')
        if kwargs.get('filePath',False):
            parentFolderName, fileName = kwargs.get('filePath').rsplit('/', 1)
        crossCheck = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{parentFolderName}/{fileName}:/"
        if kwargs.get('fileURL', False):
            base64_encoded_link = (
                base64.urlsafe_b64encode(kwargs.get('fileURL').encode("utf-8"))
                .decode("utf-8")
                .rstrip("=")
            )
            crossCheck = f"https://graph.microsoft.com/v1.0/shares/u!{base64_encoded_link}/driveItem"
        result = requests.get(crossCheck, headers=header)
        if result.status_code != 200:
            return False, f"Error fetching data: {result.status_code} - {result.text}"
        if kwargs.get('get_download_url',False):
            return result.json().get('@microsoft.graph.downloadUrl')
        if kwargs.get('get_filename', False):
            return result.json().get('name')
        checkID = re.search(r'\{(.+?)\}', result.json().get('eTag')).group(1).lower()
        return checkID
    
    def read_item2(self, **kwargs):
        """
        Reads and processes a file from a SharePoint site based on its type.
        
        Args:
            kwargs: 
                - group (str, optional): The group channel name (default is P-Data Management DP).
                - fileName (str, optional): The name of the file to read. Required if `filePath` or `fileURL` is not provided.
                - parentFolderName (str, optional): The folder containing the file. Required if `filePath` or `fileURL` is not provided.
                - filePath (str, optional): The file path of the file. Required if `fileName` and `parentFolderName` is not provided or `fileURL` is not provided.
                - fileURL (str, optional): The file shared url of the file. Required if `fileName` and `parentFolderName` is not provided or `filePath` is not provided.
                - sheet_name (str, optional): The sheet name to read from (for Excel files). (Default is 'Sheet1').
                - separator (str, optional): The separator to use for CSV/TXT files. (Default is ';').
                - skip_row (int, optional): Rows to skip for TXT files. (Default is 1).

        Returns:
            DataFrame: Pandas DataFrame with the file contents.
        """
        site_id, list_id = self.check_group(**kwargs)
        if not kwargs.get('fileName') and not kwargs.get('parentFolderName') and not kwargs.get('filePath') and not kwargs.get('fileURL'):
            return False, 'There is no fileName, parentFolderName or fileURL to search!'
        header = self.get_token(**kwargs)
        fileName = kwargs.get('fileName')
        parentFolderName = kwargs.get('parentFolderName')
        if kwargs.get('filePath', False):
            parentFolderName, fileName = kwargs.get('filePath').rsplit('/', 1)
        requests_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{parentFolderName}/{fileName}:/content"
        if kwargs.get('fileURL', False):
            encoded_shared_url = (
                base64.urlsafe_b64encode(kwargs.get('fileURL').encode("utf-8"))
                .decode("utf-8")
                .rstrip("=")
            )
            temp_requests_url = f"https://graph.microsoft.com/v1.0/shares/u!{encoded_shared_url}/driveItem"
            kwargs['get_filename'] = True
            fileName = self.search_item_details(**kwargs)
            kwargs['get_filename'] = False
            response_temp = requests.get(temp_requests_url, headers=header)
            requests_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/items/{response_temp.json().get('id')}/content"
        response = requests.get(requests_url, headers=header)
        file_content = BytesIO(response.content)
        file_extension = fileName.split('.')[-1].lower()
        if file_extension == 'csv':   
            return pd.read_csv(file_content)
        elif file_extension == 'py':
            return self.download_item(**kwargs)
        elif file_extension == 'xlsb':
            return pd.read_excel(file_content, sheet_name=kwargs.get('sheet_name','Sheet1'), engine='pyxlsb')
        elif file_extension == 'xlsx':
            return pd.read_excel(file_content, sheet_name=kwargs.get('sheet_name','Sheet1'), engine='openpyxl')
        elif file_extension == 'zip':
            with zipfile.ZipFile(file_content, 'r') as z:
                with z.open(z.namelist()[0]) as f:
                    return pd.read_csv(f)
        elif file_extension == 'txt':
            file_content.seek(0) 
            return pd.read_csv(file_content, sep=kwargs.get('separator',';'), skiprows=kwargs.get('skip_row',1))
        return pd.read_excel(file_content, sheet_name=kwargs.get('sheet_name','Sheet1'))
