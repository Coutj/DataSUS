import os
from azure.storage.filedatalake import (
    DataLakeServiceClient,
    DataLakeDirectoryClient,
    FileSystemClient
)
from azure.identity import DefaultAzureCredential
from pathlib import Path

class ADLS:

    def __init__(self, account_name,  sas_token, container_name):
        self.account_name = account_name
        self.sas_token = sas_token
        self.service_client = self.get_service_client_sas()
        self.file_system_client = self.service_client.get_file_system_client(container_name)


    def get_service_client_sas(self) -> DataLakeServiceClient:
        account_url = f"https://{self.account_name}.dfs.core.windows.net"

        # The SAS token string can be passed in as credential param or appended to the account URL
        service_client = DataLakeServiceClient(account_url, credential=self.sas_token)

        return service_client


    def list_directory_contents(self, directory_name: str):
        paths = self.file_system_client.get_paths(path=directory_name, recursive=False)

        dir_list = []
        for path in paths:
            dir_list.append(path.name)
            print(path.name + '\n')

        return dir_list


    def create_directory(self, directory_name: str) -> DataLakeDirectoryClient:
        directory_client = self.file_system_client.create_directory(directory_name)

        return directory_client
    
    def upload_file_to_directory(self, directory_client: DataLakeDirectoryClient, local_path: str, file_name: str):
        file_client = directory_client.get_file_client(file_name)

        with open(file=os.path.join(local_path, file_name), mode="rb") as data:
            file_client.upload_data(data, overwrite=True)


    def upload_folder_to_directory(self, local_dir_path: str, destin_dir: str):
        
        dir_name = Path(local_dir_path).name
        dir_client = self.create_directory(os.path.join(destin_dir, dir_name))
        for file in Path(local_dir_path).glob("*"):
            file_client = dir_client.get_file_client(file.name)

            with open(file=file.absolute(), mode="rb") as data:
                file_client.upload_data(data, overwrite=True)
