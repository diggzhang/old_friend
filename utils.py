import os
import toml
import json
import datetime
import requests
from impala.dbapi import connect


class PostgresDriver(object):
    def __init__(self, host, port, db, user, password):
        connect_info_str = "host={host} port={port} dbname={dbname} user={user} password={password}".format(
            host=host,
            port=port,
            dbname=db,
            user=user,
            password=password,
        )
        try:
            self.db = db
            self.client = psycopg2.connect(connect_info_str)
        except Exception as e:
            raise e

    def connection_info(self):
        print(self.client)

    def count(self, tables_name, query_sql_str=None):
        if query_sql_str is None:
            query_sql_str = """
                select count(1) from public.{table_name}
            """
        tables_list_of_count = []
        for table in tables_name:
            try:
                cursor = self.client.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
                cursor.execute(query_sql_str.format(
                    table_name=table
                ))
                tables_list_of_count.append({
                    self.db + '.' + table: cursor.fetchone()[0]
                })
                cursor.close()
            except Exception as e:
                self.client.close()
                raise e
        return tables_list_of_count

    def sql(self, query_sql_str):
        if query_sql_str is None:
            raise Exception("postgre_sql_executer query_sql_str must one string of query sql.")

        try:
            cursor = self.client.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
            cursor.execute(query_sql_str)
            result = cursor.fetchall()
            cursor.close()
            return result
        except Exception as e:
            self.client.close()
            raise e

    def sql_no_results(self, query_sql_str):
        if query_sql_str is None:
            raise Exception("postgre_sql_executer query_sql_str must one string of query sql.")

        try:
            cursor = self.client.cursor()
            cursor.execute(query_sql_str)
            cursor.close()
            self.client.commit()
            return True
        except Exception as e:
            self.client.close()
            raise e

    def connection_close(self):
        try:
            self.client.close()
        except Exception as e:
            raise e
            

class HiveDriver(object):
    def __init__(self, host, port, user, password):
        try:
            self.client = connect(host=host,
                                  port=port,
                                  user=user,
                                  password=password,
                                  auth_mechanism="PLAIN")
        except Exception as e:
            raise Exception(e)

    def connection_info(self):
        print(self.client)

    def sql(self, query_sql_str):
        if query_sql_str is None:
            raise Exception("hive_sql_executer query_sql_str must one string of query sql.")

        try:
            cursor = self.client.cursor()
            cursor.execute(query_sql_str)
            result = cursor.fetchall()
            cursor.close()
            return result
        except Exception as e:
            self.client.close()
            raise Exception(e)

    def sql_no_result(self, query_sql_str):
        if query_sql_str is None:
            raise Exception("hive_sql_executer query_sql_str must one string of query sql.")

        try:
            cursor = self.client.cursor()
            cursor.execute(query_sql_str)
            cursor.close()
        except Exception as e:
            self.client.close()
            raise Exception(e)

    def connection_close(self):
        try:
            self.client.close()
            print('Hive connection closed')
        except Exception as e:
            raise Exception(e)


class DateUtil(object):
    def __init__(self, input_datetime):
        self.date = None
        if input_datetime is not None:
            self.date = input_datetime
        else:
            self.date = datetime.datetime.now() - datetime.timedelta(days=1)

    def yyyymmdd(self):
        return self.date.strftime("%Y%m%d")

    def start_day_of_month(self):
        return datetime.datetime(self.date.year, self.date.month, 1).strftime("%Y%m%d")

    def last_month_first_day(self):
        return datetime.datetime(self.date.year, self.date.month - 1, 1).strftime("%Y%m%d")


def ding(data):
    date_instance = DateUtil(None)
    date_is = date_instance.yyyymmdd()
    headers = {'Content-Type': 'application/json'}
    content = {
        "title": date_is + "丢失用户补回",
        "text": "### " + date_is + "丢失用户统计 \n" + data
    }
    post_data = {
        "msgtype": "markdown",
        "markdown": content
    }
    try:
        requests.post(load_params(param="ding")["bot_url"], headers=headers, data=json.dumps(post_data))
    except IOError as e:
        raise Exception(e)


def load_params(config_file=os.path.abspath('.') + '/config.toml', param=""):
    """
    Load parameters from file.
    If config file doesn't exist, we ask user to build one
    """
    params = {}
    if not os.path.isfile(config_file):
        raise Exception("toml config file not found.")

    if not params:
        with open(config_file, 'r', encoding='utf-8') as fp:
            params.update(toml.load(fp))

    if param:
        return params[param]
    else:
        return params


def open_csv(csv_file):
    import csv
    from collections import namedtuple
    file_context = []
    with open(csv_file) as file_object:
        f_csv = csv.reader(file_object)
        headings = next(f_csv)
        Row = namedtuple('Row', headings)
        for r in f_csv:
            row = Row(*r)
            file_context.append(row)
    return file_context


