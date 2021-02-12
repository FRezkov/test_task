import concurrent.futures as cf
from modules.psqlModules import psql_execute


def chunks(array, start, num):
    """Yield successive n-sized chunks from array"""
    for i in range(start, len(array), num):
        yield array[i:i + num]


def execute_concurent_process(db_conn, query_list, nprocs):
   """The parallel process of executing queries"""

   print('Start parallel sending...')

   with cf.ThreadPoolExecutor(max_workers=nprocs) as executor:
       future_to_query = {executor.submit(psql_execute, 
					  db_conn, 
					  query): query for query in query_list}
       for future in cf.as_completed(future_to_query):
#            func_query = future_to_query[future]
           print(future.result())


