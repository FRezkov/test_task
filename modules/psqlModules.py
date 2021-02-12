import psycopg2
from psycopg2.extras import RealDictCursor
from configparser import ConfigParser


def config(section, filename, show=False):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
        db['connect_timeout'] = 5
        if show:
            print(db)
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, 
                                                                       filename))

    return db

def psql_execute(db_conn, query, fetch = False):
    # Define connection propeties
    conn_str = 'postgresql://'+db_conn['user']+':'+db_conn['password']+ \
             '@'+db_conn['host']+':'+db_conn['port']+'/'+db_conn['database']
    # Execute DML command on db
    engine = psycopg2.connect(conn_str)
    with engine.cursor() as cur:
    	cur.execute(query)
    	rows_count = cur.rowcount
    	engine.commit()
    	cur.close()
    	engine.close()
    return rows_count


def psql_fetch(db_conn, query, **kwargs):
    # Define connection propeties
    conn_str = 'postgresql://'+db_conn['user']+':'+db_conn['password']+ \
             '@'+db_conn['host']+':'+db_conn['port']+'/'+db_conn['database']
    # Execute DML command on db
    engine = psycopg2.connect(conn_str)
    with engine.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, kwargs)
        return cur.fetchall()
