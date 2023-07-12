import asyncio
import glob
import json
import os
from datetime import datetime, timedelta, time as dtime
from ftplib import FTP_TLS
import socket
from typing import List

import inflection
import pandas as pd

import asyncpg
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator

#made by Grelliks hands and tears

args = {
    'owner': 'airflow'
}

dag = DAG(
    dag_id='samsonite_demo',
    default_args=args,
    start_date=datetime(2021, 4, 1),
    schedule_interval=None,  # timedelta(hours=1),
    catchup=False,
    tags=['samsonite'],
)


def get_list_of_files(ftp: FTP_TLS, data_type: str) -> List[str]:
    ftp.cwd('/' + data_type)
    return ftp.nlst()


async def get_last_downloaded_file(conn: asyncpg.Connection, data_type: str) -> str:
    query = "select filename from last_downloaded_files where task_id = $1;"
    return await conn.fetchval(query, 'download_' + data_type)


async def get_last_saved_file(conn: asyncpg.Connection, data_type: str) -> str:
    query = "select filename from last_downloaded_files where task_id = $1;"
    return await conn.fetchval(query, 'db_saved_' + data_type)


async def update_last_downloaded_file(conn: asyncpg.Connection, data_type: str, last_file: str) -> None:
    query = "UPDATE last_downloaded_files SET filename = $1 WHERE task_id = $2;"
    await conn.execute(query, last_file, 'download_' + data_type)


async def update_last_saved_file(conn: asyncpg.Connection, data_type: str, last_file: str, insert=False) -> None:
    if insert:
        query = "insert into last_downloaded_files values ($2, $1);"
    else:
        query = "UPDATE last_downloaded_files SET filename = $1 WHERE task_id = $2;"
    await conn.execute(query, last_file, 'db_saved_' + data_type)


async def ftp_downloader_task(data_type, **kwargs):
    output_path = Variable.get('local_storage_samsonite_path')
    pg_connection = BaseHook.get_connection('samsonite_main_db')
    ftp_connection = BaseHook.get_connection('samsonite_ftp')
    ftp_host = ftp_connection.host
    ftp_user = ftp_connection.login
    ftp_pass = ftp_connection.password
    connection_url = pg_connection.get_uri()
    conn = await asyncpg.connect(connection_url)
    try:
        with FTP_TLS(ftp_host) as ftp:
            ftp.sendcmd(f'USER {ftp_user}')
            ftp.sendcmd(f'PASS {ftp_pass}')
            ftp.af = socket.AF_INET6
            files = get_list_of_files(ftp, data_type)
            last_file = await get_last_downloaded_file(conn, data_type)
            if last_file is None:
                os.mkdir(os.path.join(output_path, data_type))
            else:
                files = [x for x in files if x > last_file]
            files.sort()
            for idx, f in enumerate(files):
                filename = os.path.join(output_path, data_type, f)
                with open(filename, 'wb') as file:
                    ftp.retrbinary("RETR " + f, file.write)
                    last_file = f
                print(f"complete {idx + 1} / {len(files)}")
                if (idx + 1) % 10:
                    await update_last_downloaded_file(conn, data_type, last_file)
            ftp.quit()
    finally:
        await update_last_downloaded_file(conn, data_type, last_file)
        await conn.close()


async def save_raw_to_db_receipthead(data_type, **kwargs):
    output_path = Variable.get('local_storage_samsonite_path')
    pg_connection = BaseHook.get_connection('samsonite_main_db')
    connection_url = pg_connection.get_uri()
    conn = await asyncpg.connect(connection_url)
    last_file = await get_last_saved_file(conn, data_type)
    all_files = glob.glob(os.path.join(output_path, data_type, '*.json'))
    all_files = sorted([os.path.basename(x) for x in all_files])
    if last_file is None:
        new_files = all_files
    else:
        new_files = [x for x in all_files if x > last_file]
    try:
        for idx, f in enumerate(new_files):
            print(f"{idx + 1} / {len(new_files)} {f}")
            with open(os.path.join(output_path, data_type, f)) as file:
                data = json.load(file)
            df = pd.DataFrame(data)
            df['receiptDate'] = pd.to_datetime(df['receiptDate'])
            insert_flag = (last_file is None) and (idx == 0)
            async with conn.transaction():
                query = """
                    delete from receipt_head
                    WHERE receipt_no = ANY($1::text[]);
                """
                await conn.execute(query, list(df.receiptNo.unique()))
                columns = [inflection.underscore(x) for x in df.columns.to_list()]
                await conn.copy_records_to_table('receipt_head', records=df.values, columns=columns)
                await update_last_saved_file(conn, data_type, f, insert_flag)
    finally:
        await conn.close()


async def save_raw_to_db_receiptmain(data_type, **kwargs):
    output_path = Variable.get('local_storage_samsonite_path')
    pg_connection = BaseHook.get_connection('samsonite_main_db')
    connection_url = pg_connection.get_uri()
    conn = await asyncpg.connect(connection_url)
    last_file = await get_last_saved_file(conn, data_type)
    all_files = glob.glob(os.path.join(output_path, data_type, '*.json'))
    all_files = sorted([os.path.basename(x) for x in all_files])
    if last_file is None:
        new_files = all_files
    else:
        new_files = [x for x in all_files if x > last_file]
    try:
        for idx, f in enumerate(new_files):
            print(f"{idx + 1} / {len(new_files)} {f}")
            with open(os.path.join(output_path, data_type, f)) as file:
                data = json.load(file)
            df = pd.DataFrame(data)
            df['goodsId'] = df['goodsId'].apply(int)
            df['personId'] = df['personId'].apply(int)
            insert_flag = (last_file is None) and (idx == 0)
            async with conn.transaction():
                query = """
                    delete from receipt_main
                    WHERE receipt_id = ANY($1::text[]);
                """
                await conn.execute(query, list(df.receiptId.unique()))
                columns = [inflection.underscore(x) for x in df.columns.to_list()]
                await conn.copy_records_to_table('receipt_main', records=df.values, columns=columns)
                await update_last_saved_file(conn, data_type, f, insert_flag)
    finally:
        await conn.close()


async def save_raw_to_db_traffic(data_type, **kwargs):
    output_path = Variable.get('local_storage_samsonite_path')
    pg_connection = BaseHook.get_connection('samsonite_main_db')
    connection_url = pg_connection.get_uri()
    conn = await asyncpg.connect(connection_url)
    last_file = await get_last_saved_file(conn, data_type)
    all_files = glob.glob(os.path.join(output_path, data_type, '*.json'))
    all_files = sorted([os.path.basename(x) for x in all_files])
    if last_file is None:
        new_files = all_files
    else:
        new_files = [x for x in all_files if x > last_file]
    try:
        for idx, f in enumerate(new_files):
            print(f"{idx + 1} / {len(new_files)} {f}")
            insert_flag = (last_file is None) and (idx == 0)
            with open(os.path.join(output_path, data_type, f)) as file:
                data = json.load(file)
                for ddata in data:
                    traffic_date = pd.to_datetime(ddata['trafficDate'])
                    unit_id = ddata['unitId']
                    df = pd.DataFrame(ddata['trafficData'])
                    if len(df) == 0:
                        continue
                    df['traffic_date'] = traffic_date
                    df['unit_id'] = unit_id
                    df['trafficTime'] = df['trafficTime'].apply(lambda x: dtime(*map(int, x.split(':'))))
                    async with conn.transaction():
                        query = """
                            delete from traffic
                            WHERE unit_id = $1 and traffic_date = $2;
                        """
                        await conn.execute(query, unit_id, traffic_date)
                        columns = [inflection.underscore(x) for x in df.columns.to_list()]
                        await conn.copy_records_to_table('traffic', records=df.values, columns=columns)
            await update_last_saved_file(conn, data_type, f, insert_flag)
    finally:
        await conn.close()


async def save_raw_to_db_cashbox(data_type, **kwargs):
    output_path = Variable.get('local_storage_samsonite_path')
    pg_connection = BaseHook.get_connection('samsonite_main_db')
    connection_url = pg_connection.get_uri()
    conn = await asyncpg.connect(connection_url)
    last_file = await get_last_saved_file(conn, data_type)
    all_files = glob.glob(os.path.join(output_path, data_type, '*.json'))
    all_files = sorted([os.path.basename(x) for x in all_files])
    if last_file is None:
        new_files = all_files
    else:
        new_files = [x for x in all_files if x > last_file]
    try:
        for idx, f in enumerate(new_files):
            print(f"{idx + 1} / {len(new_files)} {f}")
            with open(os.path.join(output_path, data_type, f)) as file:
                data = json.load(file)
            df = pd.DataFrame(data)
            insert_flag = (last_file is None) and (idx == 0)
            async with conn.transaction():
                query = """
                    delete from cashbox
                    WHERE cashbox_id = ANY($1::text[]);
                """
                await conn.execute(query, list(df['cashboxId'].unique()))
                columns = [inflection.underscore(x) for x in df.columns.to_list()]
                await conn.copy_records_to_table('cashbox', records=df.values, columns=columns)
                await update_last_saved_file(conn, data_type, f, insert_flag)
    finally:
        await conn.close()


async def update_integr_kpi(**kwargs):
    pg_connection = BaseHook.get_connection('samsonite_main_db')
    connection_url = pg_connection.get_uri()
    conn = await asyncpg.connect(connection_url)
    try:
        async with conn.transaction():
            query = "delete from integr_kpi;"
            await conn.execute(query)
            query = """
                insert into integr_kpi
                select date,
                       division_id,
                       employee_id,
                       1 as kpi_id,
                       sum(value) as value
                from (
                select date(rh.receipt_date) as date,
                       unit_id as division_id,
                       rh.receipt_value as value,
                       person_id as employee_id
                from receipt_head rh
                         left join receipt_main rm on rh.receipt_id = rm.receipt_id
                         left join cashbox c on rh.cashbox_id = c.cashbox_id
                where receipt_operation_type = 'Продажа') z
                group by date, division_id, employee_id
                union all
                select date,
                       division_id,
                       employee_id,
                       2 as kpi_id,
                       sum(value) as value
                from (
                select date(rh.receipt_date) as date,
                       unit_id as division_id,
                       rh.receipt_value as value,
                       person_id as employee_id
                from receipt_head rh
                         left join receipt_main rm on rh.receipt_id = rm.receipt_id
                         left join cashbox c on rh.cashbox_id = c.cashbox_id
                where receipt_operation_type = 'Возврат') z
                group by date, division_id, employee_id
                union all
                select traffic_date as date,
                       unit_id,
                       null as employee_id,
                       3 as kpi_id,
                       sum(traffic_in+traffic_out) / 2 as value
                from traffic
                group by date, unit_id;        
            """
            await conn.execute(query)
    finally:
        await conn.close()


async def update_main_kpi(**kwargs):
    pg_connection = BaseHook.get_connection('samsonite_main_db')
    connection_url = pg_connection.get_uri()
    conn = await asyncpg.connect(connection_url)
    try:
        async with conn.transaction():
            query = "delete from main_kpi;"
            await conn.execute(query)
            query = """
                insert into main_kpi
                select date,
                       division_id                                         as entity_id,
                       'division'                                          as entity_type,
                       coalesce(sum(value) filter ( where kpi_id = 1 ), 0) as kpi_revenue_value,
                       coalesce(sum(value) filter ( where kpi_id = 2 ), 0) as kpi_return_value,
                       coalesce(sum(value) filter ( where kpi_id = 3 ), 0) as kpi_traffic_value
                from integr_kpi
                group by date, division_id
                union all
                select date,
                       employee_id::varchar                                as entity_id,
                       'employee'                                          as entity_type,
                       coalesce(sum(value) filter ( where kpi_id = 1 ), 0) as kpi_revenue_value,
                       coalesce(sum(value) filter ( where kpi_id = 2 ), 0) as kpi_return_value,
                       null                                                as kpi_traffic_value
                from integr_kpi
                where kpi_id in (1, 2)
                group by date, employee_id;
            """
            await conn.execute(query)
    finally:
        await conn.close()


def run_async(**kwargs):
    loop = asyncio.get_event_loop()
    print(kwargs)
    func = kwargs.get('function')
    p = {
        'data_type': kwargs.get('data_type')
    }
    result = loop.run_until_complete(func(**p))
    return result


t_download_receipthead = PythonOperator(
    task_id='download_receipthead',
    python_callable=run_async,
    op_kwargs={
        'data_type': 'receipthead',
        'function': ftp_downloader_task
    },
    dag=dag,
)

t_download_receiptmain = PythonOperator(
    task_id='download_receiptmain',
    python_callable=run_async,
    op_kwargs={
        'data_type': 'receiptmain',
        'function': ftp_downloader_task
    },
    dag=dag,
)

t_download_cashbox = PythonOperator(
    task_id='download_cashbox',
    python_callable=run_async,
    op_kwargs={
        'data_type': 'cashbox',
        'function': ftp_downloader_task
    },
    dag=dag,
)

t_download_traffic = PythonOperator(
    task_id='download_traffic',
    python_callable=run_async,
    op_kwargs={
        'data_type': 'traffic',
        'function': ftp_downloader_task
    },
    dag=dag,
)

t_save_to_db_receipthead = PythonOperator(
    task_id='save_to_db_receipthead',
    python_callable=run_async,
    op_kwargs={
        'data_type': 'receipthead',
        'function': save_raw_to_db_receipthead
    },
    dag=dag,
)

t_download_receipthead >> t_save_to_db_receipthead

t_save_to_db_receiptmain = PythonOperator(
    task_id='save_to_db_receiptmain',
    python_callable=run_async,
    op_kwargs={
        'data_type': 'receiptmain',
        'function': save_raw_to_db_receiptmain
    },
    dag=dag,
)

t_download_receiptmain >> t_save_to_db_receiptmain

t_save_to_db_traffic = PythonOperator(
    task_id='save_to_db_traffic',
    python_callable=run_async,
    op_kwargs={
        'data_type': 'traffic',
        'function': save_raw_to_db_traffic
    },
    dag=dag,
)

t_download_traffic >> t_save_to_db_traffic

t_save_to_db_cashbox = PythonOperator(
    task_id='save_to_db_cashbox',
    python_callable=run_async,
    op_kwargs={
        'data_type': 'cashbox',
        'function': save_raw_to_db_cashbox
    },
    dag=dag,
)

t_download_cashbox >> t_save_to_db_cashbox

t_update_integr_kpi = PythonOperator(
    task_id='update_integr_kpi',
    python_callable=run_async,
    op_kwargs={
        'data_type': 'any',
        'function': update_integr_kpi
    },
    dag=dag,
)

t_save_to_db_receiptmain >> t_update_integr_kpi
t_save_to_db_receipthead >> t_update_integr_kpi
t_save_to_db_traffic >> t_update_integr_kpi
t_save_to_db_cashbox >> t_update_integr_kpi

t_update_main_kpi = PythonOperator(
    task_id='update_main_kpi',
    python_callable=run_async,
    op_kwargs={
        'data_type': 'any',
        'function': update_main_kpi
    },
    dag=dag,
)

t_update_integr_kpi >> t_update_main_kpi

if __name__ == "__main__":
    dag.cli()