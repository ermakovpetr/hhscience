# coding=utf-8
def _get_connection_mssql():
    import pymssql
    import config
    
    return pymssql.connect(database=config.ms_db_name, host=config.ms_db_host,
                           user=config.ms_db_user, password=config.ms_db_pass)


def _get_connection_postgres():
    import psycopg2
    import config
    
    return psycopg2.connect(database=config.ps_db_name, host=config.ps_db_host,
                            port=config.ps_db_port, user=config.ps_db_user,  
                            password=config.ps_db_pass)


def _get_connection_postgres_data():
    import psycopg2
    import config
    
    return psycopg2.connect(database=config.ps_dbdata_name, host=config.ps_db_host,
                            port=config.ps_dbdata_port, user=config.ps_db_user,  
                            password=config.ps_db_pass)


def _get_connection_postgres_hhid():
    import psycopg2
    import config
    
    return psycopg2.connect(database=config.ps_dbhhid_name, host=config.ps_db_host,
                            port=config.ps_dbhhid_port, user=config.ps_db_user,  
                            password=config.ps_db_pass)



def _get_connection_hive_data():
    from pyhive import hive
    import config

    return hive.connect(config.hive_db_host, port=config.hive_db_port)


def cache(write_cache=False, read_cache=False):
    READ_CACHE_PARAM = 'read_cache'
    WRITE_CACHE_PARAM = 'write_cache'
            
    def decorator(func):
        def wrapper(*args, **kwargs):
            import pickle
            import os.path
            
            fname = '__cache_' + func.func_name + '_' + str(hash(str(args) + str({
                            key: kwargs[key] for key in kwargs if key not in [READ_CACHE_PARAM, WRITE_CACHE_PARAM]
                        }))) + '.pckl'
            
            if kwargs.get(READ_CACHE_PARAM, read_cache) and os.path.exists(fname):
                print 'log: read cache'
                res = pickle.load(open(fname))
            else:
                res = func(*args, **kwargs)

            if kwargs.get(WRITE_CACHE_PARAM, write_cache):
                print 'log: write cache'
                pickle.dump(res, open(fname, 'w'))

            return res
        return wrapper
    return decorator


def connect_db(get_connection):
    def decorator(func):
        def wrapper(*args, **kwargs):
            conn = None
            try:
                conn = get_connection()
                return func(conn.cursor(), *args, **kwargs)
            except Exception as e:
                print 'Error %s' % e
            finally:
                if conn is not None:
                    conn.close()
        return wrapper
    return decorator


def parse_mssql(func, *args, **kwargs):
    def wrapper(cursor, *args, **kwargs):
        cursor = func(cursor, *args, **kwargs)
        columns = [column_name[0] for column_name in cursor.description]
        return list(cursor), columns
    return wrapper


def parse_postgres(func, *args, **kwargs):
    def wrapped(cursor, *args, **kwargs):
        cursor = func(cursor, *args, **kwargs)
        columns = [i.name for i in cursor.description]
        data = cursor.fetchall()
        return data, columns
    return wrapped


def wrap_pandas(func):
    def wrapper(cursor, *args, **kwargs):
        import pandas as pd
        data, columns = func(cursor, *args, **kwargs)
        return pd.DataFrame(data, columns=columns)
    return wrapper


@cache()
@connect_db(_get_connection_postgres)
@wrap_pandas
@parse_postgres
def load_pssql(cursor, sql, **kwargs):
    cursor.execute(sql)
    return cursor


@cache()
@connect_db(_get_connection_postgres_data)
@wrap_pandas
@parse_postgres
def load_pssql_data(cursor, sql, **kwargs):
    cursor.execute(sql)
    return cursor


@cache()
@connect_db(_get_connection_postgres_hhid)
@wrap_pandas
@parse_postgres
def load_pssql_hhid(cursor, sql, **kwargs):
    cursor.execute(sql)
    return cursor


@cache()
@connect_db(_get_connection_mssql)
@wrap_pandas
@parse_mssql
def load_mssql(cursor, sql, **kwargs):
    cursor.execute(sql)
    return cursor


@cache()
@connect_db(_get_connection_hive_data)
@wrap_pandas
@parse_mssql
def load_hive(cursor, sql, **kwargs):
    cursor.execute(sql)
    return cursor

@cache()
@connect_db(_get_connection_hive_data)
@parse_mssql
def load_hive_no_pandas(cursor, sql, **kwargs):
    cursor.execute(sql)
    return cursor

@cache()
@connect_db(_get_connection_hive_data)
@wrap_pandas
@parse_mssql
def pretty_load_hive(cursor, sql, **kwargs):
    cursor.execute(sql, async=True)
    
    import re
    from ipywidgets import IntProgress, HTML, HBox, Checkbox, VBox, Textarea, ToggleButton
    from IPython.display import display
    
    stop_button = ToggleButton(description='Stop Job')
    map_bar = IntProgress(value=0, min=0, max=100, description='Map:')
    reduce_bar = IntProgress(value=0, min=0, max=100, description='Reduce:')
    log_container = Textarea()
    show_log_checkbox = Checkbox(value=True, visible=True, description='Show Log')
    container = VBox([HBox([stop_button, map_bar, reduce_bar, show_log_checkbox]), log_container])
    log_container.width = '100%'
    display(container)
    
    all_logs = []
    status = cursor.poll()
    while status.operationState < 2 and not stop_button.value:
        log_container.visible = show_log_checkbox.value
        
        status = cursor.poll()
        logs = cursor.fetch_logs()
        log_container.value = '\n'.join(all_logs)
        if len(logs) > 0:
            for log in logs:
                all_logs.append(log)
                progress = re.findall('map\s*=\s*(\d+)%,\s*reduce\s*=\s*(\d+)%', log)
                if len(progress) > 0:
                    map_bar.value, reduce_bar.value = map(int, progress[0])

    if stop_button:
        map_bar.color = reduce_bar.color = 'red'
    else:
        map_bar.value = reduce_bar.value = 100
        map_bar.color = reduce_bar.color = 'green'
    return cursor