"""
Модуль описывает репозиторий, работающий в базе данных sqlite
"""

import sqlite3
from inspect import get_annotations
from typing import Any, Type, Optional, List

from bookkeeper.repository.abstract_repository import AbstractRepository, T


class SQLLiteRepository(AbstractRepository[T]):
    """
    Репозиторий, работающий в базе данных.
    """

    def __init__(self, db_file: str, cls: type) -> None:
        self.db_file = db_file
        self.table_name = cls.__name__.lower()
        self.fields = get_annotations(cls, eval_str=True)
        self.fields.pop('pk')
        self.cls = cls

        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.table_name}
                (pk INTEGER PRIMARY KEY, {", ".join(self.fields)})
                """
            )

    def add(self, obj: T) -> int:
        names = ', '.join(self.fields.keys())
        p = ', '.join("?" * len(self.fields))
        values = [getattr(obj, x) for x in self.fields]
        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
            cur.execute('PRAGMA foreign_keys = ON')
            cur.execute(
                f"INSERT INTO {self.table_name} ({names}) VALUES ({p})", values
            )
            obj.pk = cur.lastrowid
        con.close()
        return obj.pk

    def get(self, pk: int):
        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
            cur.execute(
                f"SELECT * FROM {self.table_name} WHERE pk = ?", (pk,)
            )
            row = cur.fetchone()
            if row is None:
                return None
            values = {k: row[k] for k in row.keys()}
            return self.cls(**values)

    def get_all(self, where: dict[str, Any] | None = None) -> List[T]:
        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
            if where is None:
                cur.execute(f"SELECT * FROM {self.table_name}")
            else:
                cond = ' AND '.join(f"{k} = ?" for k in where.keys())
                cur.execute(
                    f"SELECT * FROM {self.table_name} WHERE {cond}", list(where.values())
                )
            rows = cur.fetchall()
            objects = []
            for row in rows:
                values = [row[i + 1] for i in range(len(self.fields))]
                obj = self.cls(*values)
                obj.pk = row[0]
                objects.append(obj)
            return objects

    def update(self, obj: T) -> None:

        if self.get(obj.pk) is None:
            raise ValueError(f'No object with idx = {obj.pk} in DB.')

        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
            fields = ', '.join(f"{f} = ?" for f in self.fields)
            values = [getattr(obj, f) for f in self.fields]
            values.append(obj.pk)
            cur.execute(
                f"UPDATE {self.table_name} SET {fields} WHERE pk = ?", values
            )

    def delete(self, pk: int) -> None:

        if self.get(pk) is None:
            raise ValueError(f'No object with idx = {pk} in DB.')

        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
            cur.execute(
                f"DELETE FROM {self.table_name} WHERE pk = ?", (pk,)
            )

    def delete_all(self) -> None:
        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
            cur.execute(
                f"DELETE FROM {self.table_name}"
            )
            con.commit()

    
