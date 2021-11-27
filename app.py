import asyncio
import aioschedule
import logging
import os
from loader import db
from main import check_dumps, read_and_clean
from utils import check_clean_data_files, check_data_files, check_dump_files, send_file
from asyncpg.exceptions import UniqueViolationError


async def check_files_to_db():
    clean_data = os.listdir('clear_data')
    data_dump = os.listdir('dumps_data')
    dumps = os.listdir('dumps')
    for data in clean_data:
        try:
            await db.add_clean_data(file_name=data, status='Not_send')
        except UniqueViolationError:
            pass
    for data in data_dump:
        try:
            await db.add_data(file_name=data, status='Not_clean')
        except UniqueViolationError:
            pass
    for data in dumps:
        try:
            await db.add_dump(file_name=data, status='Not_unpacked')
        except UniqueViolationError:
            pass


async def check_files():
    if os.path.exists('work.tmp'):
        logging.warning('----------Something is working----------')
        return
    clean_data_name = await check_clean_data_files()
    if clean_data_name:
        logging.warning('----------Start sending data----------')
        await send_file(clean_data_name)
        return
    data_name = await check_data_files()
    if data_name:
        logging.warning('----------Start cleaning data----------')
        await read_and_clean(data_name)
        return
    dump_name = await check_dump_files()
    if dump_name:
        logging.warning('----------Start unpacking data----------')
        await check_dumps(dump_name)
    else:
        logging.warning("----------Dump files don't exist----------")


async def on_startup():
    await db.create()
    # await db.drop_dumps()
    # await db.drop_data()
    # await db.drop_clean_data()
    await db.create_table_dumps()
    await db.create_table_data()
    await db.create_clean_data()
    # Creating planner tasks
    asyncio.create_task(schedule_checker())
    # Start new task - check files
    aioschedule.every(10).minutes.do(check_files)
    asyncio.create_task(check_files_to_db())


async def schedule_checker():
    while True:
        await aioschedule.run_pending()
        await asyncio.sleep(1)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.create_task(on_startup())
    loop.run_forever()
