from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable


@dag(
    dag_id='DataSus_Import',
    # owner = 'Coutj',
    schedule='0 0 * * MON',
    start_date=datetime(2023, 11, 1),
    catchup=False,
    tags=['DataSUS', 'SIM']
)
def datasus_import():
    
    @task(task_id='Get_List')
    def get_files_list():
        import json
        from include.adls_helper import ADLS
        from itertools import product

        # ADLS connection parameters
        conn = BaseHook.get_connection("ADLS_DataSUS")
        account_name = conn.login
        extra = json.loads(conn.extra)
        sas_token = extra['sas_token']

        current_year = datetime.now().year
        years = list(range(current_year - 4, current_year + 1))

        list_ufs = ['AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 
                    'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 
                    'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO']
        
        full_list_uf_years = list(product(list_ufs, years))
        
        # list_ufs = ['RJ']
        # full_list_uf_years = [2019]


        adls_helper = ADLS(account_name, sas_token, 'datasus-data')
        dir_list = adls_helper.list_directory_contents('raw')

        list_uf_year_in_adls = [(dir_path[6:8], int(dir_path[8:12])) for dir_path in dir_list if '.parquet' in dir_path ]
        print(list_uf_year_in_adls)

        files_to_download = list(set(full_list_uf_years) - set(list_uf_year_in_adls))
        files_to_download.sort()

        # print(files_to_download)

        files_to_download = [('RJ', 2021)]

        return files_to_download


    @task(task_id='Extract')
    def extract(files_to_download):

        import tempfile
        from pysus.online_data import SIM
        
        temp_dir = tempfile.mkdtemp()

        for uf, year in files_to_download:
            print(f"UF: {uf} - Year: {year}")
            parquet_path = SIM.download(groups='cid10', states=uf, years=year, data_dir=temp_dir)
            print(f"File Downloaded: {parquet_path}")

        return temp_dir
    

    @task(task_id='Load')
    def upload_files(dir_path):

        import json
        from pathlib import Path
        from include.adls_helper import ADLS

        # ADLS connection parameters
        conn = BaseHook.get_connection("ADLS_DataSUS")
        account_name = conn.login
        extra = json.loads(conn.extra)
        sas_token = extra['sas_token']

        adls_helper = ADLS(account_name, sas_token, container_name='datasus-data')
        print('Found files:', dir_path, Path(dir_path).glob('*.parquet'))

        for dir in Path(dir_path).glob('*.parquet'):
            print(f'Uploading dir: {dir}')
            adls_helper.upload_folder_to_directory(dir, 'raw')


    @task(task_id='Cleanup')
    def cleanup(temp_dir):

        import shutil
        from pathlib import Path

        shutil.rmtree(Path(temp_dir))

        # for dir in Path('/tmp/').glob('*'):
        #     print(f'Del dir: {dir}')
        #     shutil.rmtree('/tmp/')


    @task(task_id='GetFilesToTransform')
    def get_files_to_transform():
        import json
        from include.adls_helper import ADLS
        from itertools import product
        from pathlib import Path

        # ADLS connection parameters
        conn = BaseHook.get_connection("ADLS_DataSUS")
        account_name = conn.login
        extra = json.loads(conn.extra)
        sas_token = extra['sas_token']

        adls_helper = ADLS(account_name, sas_token, 'datasus-data')
        dir_names_to_transform = adls_helper.list_directory_contents('raw')
        dir_names_to_transform = [str(Path(dir).name) for dir in dir_names_to_transform]

        dir_names_transformed = adls_helper.list_directory_contents('trusted')
        dir_names_transformed = [str(Path(dir).name) for dir in dir_names_transformed]

        dir_names_to_transform = list(set(dir_names_to_transform) - set(dir_names_transformed))
        dir_names_to_transform.sort()
        
        print(dir_names_to_transform)
        print(dir_names_transformed)

        list_opr = ['DOAC2021.parquet', 'DOAC2020.parquet']
        # dir_names_to_transform = list_opr
        dir_names_to_transform = [ {'notebook_params': {'dir_name': dir}} for dir in dir_names_to_transform if '.parquet' in dir]

        return dir_names_to_transform



    files_to_download = get_files_list()
    download_files = extract(files_to_download)
    upload = upload_files(download_files)
    clean  = cleanup(download_files)
    files_to_transform = get_files_to_transform()

    run_databricks = DatabricksRunNowOperator.partial(
            task_id="Transform",
            databricks_conn_id="databricks",
            job_id=Variable.get('databricks_jobid'),
            max_active_tis_per_dagrun=2).expand_kwargs(files_to_transform)

    

    files_to_download >> download_files >> upload >> clean >> files_to_transform >> run_databricks


datasus_import()
