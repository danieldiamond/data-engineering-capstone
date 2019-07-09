import re
import pandas as pd
from datetime import datetime, timedelta
import s3fs
import logging

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

from plugins.operators.data_quality import DataQualityOperator
from plugins.utils.helper import get_extra_from_conn
from plugins.utils import constants

aws_conn = get_extra_from_conn(constants.AirflowConnIds.S3)

PARAMS = {
          'base_bucket': constants.S3Buckets.CAPSTONE,
          'schema': constants.General.SCHEMA,
          'redshift_user': constants.AirflowConnIds.REDSHIFT,
          'aws_access_key_id': aws_conn.get('aws_access_key_id'),
          'aws_secret_access_key': aws_conn.get('aws_secret_access_key'),
          'aws_iam_role': aws_conn.get('aws_iam_role'),
          }

default_args = {
    'owner': 'danieldiamond',
    'depends_on_past': False,
    'catchup': False,
    'start_date': datetime.now(),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('etl_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval=None,
          )

etl_begin = DummyOperator(task_id='etl_begin',  dag=dag)
etl_success = DummyOperator(task_id='etl_success',  dag=dag)


# write sas codes to s3
def write_sas_codes_to_s3(*args, **kwargs):
    """
    Grabs the codes from SAS data and save to S3 as CSV files.
    """
    s3 = s3fs.S3FileSystem(anon=False,
                           key=PARAMS['aws_access_key_id'],
                           secret=PARAMS['aws_secret_access_key'])

    with s3.open(f"{PARAMS['base_bucket']}/sas_data/"
                 "I94_SAS_Labels_Descriptions.SAS", "r") as f:
        file = f.read()

    sas_dict = {}
    temp_data = []
    for line in file.split("\n"):
        line = re.sub(r"\s+", " ", line)
        if "/*" in line and "-" in line:
            k, v = [i.strip(" ") for i in line.split("*")[1]
                                              .split("-", 1)]
            k = k.replace(' & ', '_').lower()
            sas_dict[k] = {'description': v}
        elif '=' in line and ';' not in line:
            temp_data.append([i.strip(' ').strip("'").title()
                              for i in line.split('=')])
        elif len(temp_data) > 0:
            sas_dict[k]['data'] = temp_data
            temp_data = []

    sas_dict['i94cit_i94res']['df'] = pd.DataFrame(
        sas_dict['i94cit_i94res']['data'], columns=['code', 'country'])

    tempdf = pd.DataFrame(sas_dict['i94port']['data'],
                          columns=['code', 'port_of_entry'])
    tempdf['code'] = tempdf['code'].str.upper()
    tempdf[['city', 'state_or_country']] = tempdf['port_of_entry'
                                                  ].str.rsplit(',', 1,
                                                               expand=True)
    sas_dict['i94port']['df'] = tempdf

    sas_dict['i94mode']['df'] = pd.DataFrame(
        sas_dict['i94mode']['data'], columns=['code', 'transportation'])

    tempdf = pd.DataFrame(sas_dict['i94addr']['data'],
                          columns=['code', 'state'])
    tempdf['code'] = tempdf['code'].str.upper()
    sas_dict['i94addr']['df'] = tempdf

    sas_dict['i94visa']['df'] = pd.DataFrame(
        sas_dict['i94visa']['data'], columns=['code', 'reason_for_travel'])

    for table in sas_dict.keys():
        if 'df' in sas_dict[table].keys():
            logging.info(f"Writing {table} to S3")
            with s3.open(f"{PARAMS['base_bucket']}/{table}.csv", "w") as f:
                sas_dict[table]['df'].to_csv(f, index=False)


task_write_sas_codes_to_s3 = PythonOperator(
    task_id='write_sas_codes_to_s3',
    python_callable=write_sas_codes_to_s3,
    dag=dag
)

# Drop & Create Tables
for table in constants.General.TABLES:
    logging.info(f"Drop & Create {table}")
    PARAMS['table'] = table
    PARAMS['s3_uri'] = ("s3://{base_bucket}/{table}.csv".format(**PARAMS))
    drop_stmt = constants.SQLQueries.DROP_TABLE.format(**PARAMS)
    create_stmt = constants.SQLQueries.CREATE[table]
    grant_usage_stmt = constants.SQLQueries.GRANT_USAGE.format(**PARAMS)
    grant_select_stmt = constants.SQLQueries.GRANT_SELECT.format(**PARAMS)

    # Drop, Create, Grant Access Task
    task_create_table = PostgresOperator(
        task_id=f"create_{table}",
        postgres_conn_id="redshift",
        sql=[drop_stmt, create_stmt, grant_usage_stmt, grant_select_stmt],
        dag=dag
    )

    if table in constants.General.CSV_TABLES:
        PARAMS['s3_uri'] = ('s3://{base_bucket}/{table}.csv'.format(**PARAMS))
        copy_stmt = constants.SQLQueries.COPY_CSV_TABLE.format(**PARAMS)
    elif table in constants.General.PARQUET_TABLES:
        PARAMS['s3_uri'] = ('s3://{base_bucket}/parquet_data'.format(**PARAMS))
        copy_stmt = constants.SQLQueries.COPY_PARQUET_TABLE.format(**PARAMS)
    else:
        logging.info(f"WARNING: Unable to COPY {table}")
        continue

    # COPY task
    task_copy_table = PostgresOperator(
        task_id=f"copy_{table}",
        postgres_conn_id="redshift",
        sql=copy_stmt,
        dag=dag
    )
    logging.info(f"Successfully Copied {table}")

    # Data Quality Check Task
    task_data_quality = DataQualityOperator(
        task_id=f"data_quality_check_on_{table}",
        redshift_conn_id="redshift",
        table=table,
        dag=dag
    )

    task_write_sas_codes_to_s3 >> task_create_table
    task_create_table >> task_copy_table
    task_copy_table >> task_data_quality
    task_data_quality >> etl_success

etl_begin >> task_write_sas_codes_to_s3
