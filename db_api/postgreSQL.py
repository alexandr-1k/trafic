from typing import Union
import asyncpg
from asyncpg import Pool, Connection
from data import config


class Database:

    def __init__(self):
        self.pool: Union[Pool, None] = None

    async def create(self):
        self.pool = await asyncpg.create_pool(
            user=config.DB_USER,
            password=config.DB_PASS,
            host=config.DB_HOST,
            database=config.DB_NAME
        )

    async def execute(self, command, *args,
                      fetch: bool = False,
                      fetchval: bool = False,
                      fetchrow: bool = False,
                      execute: bool = False
                      ):
        async with self.pool.acquire() as connection:
            connection: Connection
            async with connection.transaction():
                if fetch:
                    result = await connection.fetch(command, *args)
                elif fetchval:
                    result = await connection.fetchval(command, *args)
                elif fetchrow:
                    result = await connection.fetchrow(command, *args)
                elif execute:
                    result = await connection.execute(command, *args)
            return result

    # Creating tables
    async def create_table_dumps(self):
        sql = """
        CREATE TABLE IF NOT EXISTS Dumps (
        id SERIAL PRIMARY KEY,
        file_name VARCHAR(255) NOT NULL UNIQUE,
        status VARCHAR(255) NOT NULL
        );
        """
        await self.execute(sql, execute=True)

    async def create_table_data(self):
        sql = """
        CREATE TABLE IF NOT EXISTS Data (
        id SERIAL PRIMARY KEY,
        file_name VARCHAR(255) NOT NULL UNIQUE,
        status VARCHAR(255) NOT NULL
        );
        """
        await self.execute(sql, execute=True)

    async def create_clean_data(self):
        sql = """
        CREATE TABLE IF NOT EXISTS Clean_data (
        id SERIAL PRIMARY KEY,
        file_name VARCHAR(255) NOT NULL UNIQUE,
        status VARCHAR(255) NOT NULL
        );
        """
        await self.execute(sql, execute=True)

    @staticmethod
    def format_args(sql, parameters: dict):
        sql += " AND ".join([
            f"{item} = ${num}" for num, item in enumerate(parameters.keys(), start=1)
        ])
        return sql, tuple(parameters.values())

    # Add to db
    async def add_dump(self, file_name, status):
        sql = """INSERT INTO Dumps(file_name, status) VALUES($1, $2) returning *"""
        return await self.execute(sql, file_name, status, fetchrow=True)

    async def add_data(self, file_name, status):
        sql = """INSERT INTO Data(file_name, status) VALUES($1, $2) returning *"""
        return await self.execute(sql, file_name, status, fetchrow=True)

    async def add_clean_data(self, file_name, status):
        sql = """INSERT INTO Clean_data(file_name, status) VALUES($1, $2) returning *"""
        return await self.execute(sql, file_name, status, fetchrow=True)

    # Select all from db
    async def select_all_dumps(self):
        sql = "SELECT * FROM Dumps"
        return await self.execute(sql, fetch=True)

    async def select_all_data(self):
        sql = "SELECT * FROM Data"
        return await self.execute(sql, fetch=True)

    async def select_all_clean_data(self):
        sql = "SELECT * FROM Clean_data"
        return await self.execute(sql, fetch=True)

    # Select from db
    async def select_dump(self, **kwargs):
        sql = "SELECT * FROM Dumps WHERE "
        sql, parameters = self.format_args(sql, parameters=kwargs)
        return await self.execute(sql, *parameters, fetchrow=True)

    async def select_data(self, **kwargs):
        sql = "SELECT * FROM Data WHERE "
        sql, parameters = self.format_args(sql, parameters=kwargs)
        return await self.execute(sql, *parameters, fetchrow=True)

    async def select_clean_data(self, **kwargs):
        sql = "SELECT * FROM Clean_data WHERE "
        sql, parameters = self.format_args(sql, parameters=kwargs)
        return await self.execute(sql, *parameters, fetchrow=True)

    # Update db data
    async def update_dump_status(self, status, file_name):
        sql = "UPDATE Dumps SET status=$1 WHERE file_name=$2"
        return await self.execute(sql, status, file_name, execute=True)

    async def update_data_status(self, status, file_name):
        sql = "UPDATE Data SET status=$1 WHERE file_name=$2"
        return await self.execute(sql, status, file_name, execute=True)

    async def update_clean_data_status(self, status, file_name):
        sql = "UPDATE Clean_data SET status=$1 WHERE file_name=$2"
        return await self.execute(sql, status, file_name, execute=True)

    # Delete all data
    async def delete_all_dumps(self):
        await self.execute("DELETE FROM Dumps WHERE True")

    async def delete_all_data(self):
        await self.execute("DELETE FROM Data WHERE True")

    async def delete_all_clean_data(self):
        await self.execute("DELETE FROM Clean_data WHERE True")

    # Delete sth in db
    async def delete_dump(self, file_name):
        await self.execute(f"DELETE FROM Dumps WHERE file_name={file_name}", execute=True)

    async def delete_data(self, file_name):
        await self.execute(f"DELETE FROM Data WHERE file_name={file_name}", execute=True)

    async def delete_clean_data(self, file_name):
        await self.execute(f"DELETE FROM Clean_data WHERE file_name={file_name}", execute=True)

    # Удаление таблицы
    async def drop_dumps(self):
        await self.execute("DROP TABLE Dumps", execute=True)

    async def drop_data(self):
        await self.execute("DROP TABLE Data", execute=True)

    async def drop_clean_data(self):
        await self.execute("DROP TABLE Clean_data", execute=True)
