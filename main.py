import traceback

import pandas as pd
import numpy as np
from math import floor
import logging
import tqdm
import os
import subprocess

from loader import db


async def save_file(df: pd.DataFrame, path, exel=False):
    if exel:
        logging.warning('Начало записи в xls')
        df.to_csv(path)
        logging.warning('Конец записи в xls')
        return
    logging.warning('Начало записи в csv')
    df.to_csv(path)
    logging.warning('Конец записи в csv')


async def check_dumps(dump):
    work_file = open('work.tmp', "w")
    try:
        dump_name = dump.split('.')[0]
        logging.warning('Начало распаковки дампа')
        inputdir = "C:\\Program Files\\Wireshark\\tshark.exe"
        # tsharkIn = open(f'C:\\Users\\USSRER\\Desktop\\study\\trafic\\dumps\\{dump}', 'rb')
        tsharkOut = open(f"C:\\Users\\USSRER\\Desktop\\study\\trafic\\dumps_data\\{dump_name}.csv", 'wb')

        tsharkCall = [inputdir, '-r', f'C:\\Users\\USSRER\\Desktop\\study\\trafic\\dumps\\{dump}', "-T", "fields",
                      "-e",
                      "frame.time_relative",
                      "-e", "ip.src", "-E", "header=y",
                      "-E", "separator=,", "-E", "quote=d"
                      ]
        subprocess.call(tsharkCall, stdout=tsharkOut)
        logging.warning('Конец распаковки дампа')
        work_file.close()
        os.remove('work.tmp')
        await db.update_dump_status(file_name=dump, status='Unpacked')
        await db.add_data(file_name=f'{dump_name}.csv', status='Not_clean')

    except Exception as e:
        print(traceback.format_exc())
        work_file.close()
        os.remove('work.tmp')


async def read_and_clean(file_name):
    # Settings for logging
    logging.basicConfig(format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
    logging.warning('Загрузка данных')
    # Read dump file
    data = pd.read_csv(f'dumps_data/{file_name}', encoding='utf8', engine='python', error_bad_lines=False)
    logging.warning('Загрузка данных прошла успешно')
    # Find unique ip in dump
    unique_ip = data['ip.src'].unique()
    len_unique_ip = len(unique_ip)
    logging.warning(f'Количество уникальных ip - {len_unique_ip}')
    logging.warning('Формирование массива')
    # Creating rows
    rows = np.array([np.zeros(len_unique_ip, dtype=np.int_)] * 901).T
    # Creating columns
    cols = np.arange(901)
    # np.append(cols, 'sum')
    # Creating dataframe
    df = pd.DataFrame(rows, index=unique_ip, columns=cols)
    logging.warning('Формирование массива закончено')
    logging.warning('Начало заполнения массива')
    # Insert data from our dump file to dataframe
    for index, row in tqdm.tqdm(data.iterrows()):
        time = row['frame.time_relative']
        ip = row['ip.src']
        df[int(floor(time))][ip] += 1
    logging.warning('Заполнение массива закончено')
    cols = np.arange(900)
    logging.warning('Суммирование всех пакетов')
    for row in unique_ip:
        sum = 0
        for col in cols:
            sum += df[col][row]
        df[900][row] = sum
    logging.warning('Суммирование закончено')
    logging.warning('Сортировка пакетов')
    df = df.sort_values(900, ascending=False)
    logging.warning('Сортировка закончена')
    # Save all file
    logging.warning('Сохранение файлов')
    name = file_name.split('.')[0]
    path = f'clear_data/{name}_clear_total.csv'
    await save_file(df=df, path=path)
    df = df.iloc[:100, 900]
    # Save only first 100 ip
    path = f'clear_data/{name}_clear.csv'
    await save_file(df=df, path=path, exel=True)
    await db.update_data_status(file_name=file_name, status='Clean')
    await db.add_clean_data(file_name=f'{name}_clear_total.csv', status='Not_send')
    await db.add_clean_data(file_name=f'{name}_clear.csv', status='Not_send')
    logging.warning('Сохранение закончено')
