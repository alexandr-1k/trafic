import asyncio

from loader import db
from pyrogram import Client
from data.config import API_HASH, API_ID, MAIN_CHANNEL
import logging


async def send_file(file_name):
    app = Client(session_name='my_account', api_hash=API_HASH, api_id=API_ID)
    await app.start()
    logging.warning('Открытие документа')
    file = open(f'clear_data/{file_name}', 'rb')
    logging.warning('Отправка документа')
    await app.send_document(chat_id=MAIN_CHANNEL, document=file)
    await asyncio.sleep(2)
    await app.stop()
    await db.update_clean_data_status(file_name=file_name, status='Send')


async def check_clean_data_files():
    clean_files = await db.select_all_clean_data()
    for file in clean_files:
        file = dict(file)
        if file.get('status') == 'Not_send':
            return file.get('file_name')
    return False


async def check_data_files():
    clean_files = await db.select_all_data()
    for file in clean_files:
        file = dict(file)
        if file.get('status') == 'Not_clean':
            print(file.get('file_name'))
            return file.get('file_name')
    return False


async def check_dump_files():
    clean_files = await db.select_all_dumps()
    for file in clean_files:
        file = dict(file)
        if file.get('status') == 'Not_unpacked':
            return file.get('file_name')
    return False
