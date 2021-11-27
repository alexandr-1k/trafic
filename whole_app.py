import asyncio.subprocess
import os
import subprocess
from typing import Union
from numpy import array_split, array, ceil, arange, floor, zeros
import pandas as pd
from tqdm import tqdm


async def func(dump, dumps_dir):

    dump_name = dump.split('.')[0]
    inputdir = "C:/Program Files/Wireshark/tshark.exe"
    tsharkOut = open(f"{dumps_dir}{dump_name}.csv", 'wb')

    tsharkCall = [inputdir, '-r', f'{dumps_dir}{dump}', "-T", "fields",
                  "-e",
                  "frame.time_relative",
                  "-e", "ip.src", "-E", "header=y",
                  "-E", "separator=,", "-E", "quote=d"
                  ]

    process = subprocess.Popen(tsharkCall, stdout=tsharkOut)

    while process:
        code = process.poll()
        if code is not None:
            break
        else:
            await asyncio.sleep(1)


async def event_loop(dumps: Union[list, tuple, str], dumps_dir:str, threads: int = 1):

    if isinstance(dumps, str):
        dumps = [dumps]

    if len(dumps) <= threads:
        dumps_by_threads = array([dumps])
    else:
        dumps_by_threads = array_split(dumps, ceil(len(dumps) / threads))

    for dumps_ in dumps_by_threads:
        tasks = [func(d, dumps_dir) for d in dumps_]
        await asyncio.gather(*tasks, return_exceptions=True)


def unpack_my_tshark_dumps(dumps: Union[str, tuple, list], dumps_dir:str, threads:int = 1):
    asyncio.run(event_loop(dumps, dumps_dir, threads))


if __name__ == '__main__':

    d = ["200501311400.dump.gz", "200901291800.dump.gz", "200106241400.dump.gz"]
    dumps_dir = "C:\\data\\MAWI\\"
    threads = 3
    unpack_my_tshark_dumps(d, dumps_dir, threads)

    for file in os.listdir(dumps_dir):
        if file.split('.')[-1] == "csv":
            data = pd.read_csv(f'{dumps_dir}{file}', encoding='utf8', engine='python', error_bad_lines=False)

            unique_ip = data['ip.src'].unique()
            len_unique_ip = len(unique_ip)
            rows = array([zeros(len_unique_ip, dtype=int)] * 901).T
            cols = arange(901)
            df = pd.DataFrame(rows, index=unique_ip, columns=cols)
            for index, row in tqdm(data.iterrows()):
                time = row['frame.time_relative']
                ip = row['ip.src']
                df[int(floor(time))][ip] += 1
            cols = arange(900)
            for row in unique_ip:
                sum = 0
                for col in cols:
                    sum += df[col][row]
                df[900][row] = sum
            df = df.sort_values(900, ascending=False)
            path = f'clear_data/{file.split(".")[0]}_clear_total.csv'
            df = df.iloc[:100, 900]

