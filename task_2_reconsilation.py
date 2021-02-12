#!/usr/bin/env python
# coding: utf-8

import concurrent.futures as cf
from modules.psqlModules import psql_execute, psql_fetch, config
from modules.thread_tools import chunks, execute_concurent_process
from modules.utils import _logger, get_parameter


def create_tables(db_conn, log, **kwgs):
    '''Create target and source tables'''
    
    # Drop previous meta data
    psql_execute(db_conn, 
                 kwgs['drop_schema'].format(kwgs['schema_raw']))
    
    # Create schema
    psql_execute(db_conn, 
                 kwgs['create_schm_query'].format(kwgs['schema_raw']))
    
    # Create tables
    for table in (kwgs['src_table1'], kwgs['src_table2']):
        psql_execute(db_conn, 
                     kwgs['create_tbl_query'].format(kwgs['schema_raw'],
                                                     table, 
                                                     kwgs['pk']))
        '''
        Count number of partition values.
        Max partition number is 100, it's a reasonable limit.
        Max rows per partition can be increased and depend on system performance.
        
        '''
        tbl_part_number = kwgs['row_count']//kwgs['rows_per_partition']
        
        if tbl_part_number > 100:
            tbl_part_number = 100
        
        # Create table's partitions
        for i in range(tbl_part_number):
            psql_execute(db_conn, 
                         kwgs['create_tbl_partition'].format(
                                                    kwgs['schema_raw'],
                                                    table,
                                                    i,
                                                    tbl_part_number))
            
    log.info('Create target and source tables completed.')


# In[25]:


def test_data_populating(db_conn, log, **kwgs):
    '''Populating tables with test data.'''
        
    # Get chunks for multiprocessing
    gen_chunk = chunks(range(0, kwgs['row_count']), 1, 
                    kwgs['rows_per_partition'])
    
    id_ranges = [[g.start, 
                  g.stop-1 if g.stop != kwgs['row_count'] else g.stop] 
                 for g in list(gen_chunk)]

    # Get list of insert queries
    ins_queries = []
    for ids in id_ranges:
        ins_queries.append(
                 kwgs['test_data_src_fill'].format(kwgs['schema_raw'],
                                               kwgs['src_table1'],
                                               kwgs['start_ts'],
                                               kwgs['end_ts'],
                                               kwgs['row_count']/2,
                                               ids[0], ids[1]))

    # Run multiinsert for source table
    execute_concurent_process(db_conn, ins_queries, kwgs['max_workers'])
    
    # Get source table partitons 
    source_prt = psql_fetch(db_conn, 
                     kwgs['get_tbl_partition'].format(kwgs['schema_raw'],
                                                     kwgs['src_table1']))
    
    # Copy data from source to target table
    ins_queries = []
    part_list = []
    for part in source_prt:
        ins_queries.append(
             kwgs['test_data_trg_fill'].format(part['prt'].replace('source',
                                                                   'target'),
                                               part['prt']))
    # Run multiinsert for target table
    execute_concurent_process(db_conn, ins_queries, kwgs['max_workers'])
        
    # Create pk for both tables 
    for table in (kwgs['src_table1'], kwgs['src_table2']):
        psql_execute(db_conn, 
                     kwgs['create_tbl_pk'].format(kwgs['schema_raw'],
                                                  table, 
                                                  kwgs['pk']))
        
    log.info('Populating tables with test data completed.')
    
    return source_prt


def test_data_modification(db_conn, log, **kwgs):
    """ Modify data in target table """
    
    # Delete random rows
    psql_execute(db_conn, 
                 kwgs['trg_tbl_random_delete'].format(kwgs['schema_raw'],
                                                     kwgs['src_table2']))
    # Update random rows
    psql_execute(db_conn, 
                 kwgs['trg_tbl_random_update'].format(kwgs['schema_raw'],
                                                     kwgs['src_table2']))
    
    log.info('Modify data in target table completed.')


def test_data_reconsilation(db_conn, log, **kwgs):
    """ Data reconsilation """

    # Create schema if not exists
    psql_execute(db_conn, 
                 kwgs['create_schm_query'].format(kwgs['schema_stg']))
    
    
    # Drop table if exists
    psql_execute(db_conn, 
                 kwgs['drop_table'].format(kwgs['schema_stg'], 
                                           kwgs['res_table']))
    
    # Create reuslt table 
    psql_execute(db_conn, 
                 kwgs['create_result_tbl_query'].format(kwgs['schema_stg'],
                                                        kwgs['res_table'])
                                                        )
    # Insert reconsilation data to result table
    ins_queries = []
    for part in kwgs['partitons']:
        ins_queries.append(
             kwgs['reconsil_query'].format(kwgs['schema_stg'],
                                           kwgs['res_table'],
                                           part['prt'],
                                           part['prt'].replace('source',
                                                               'target') ))

    # Run multiinsert for result table
    execute_concurent_process(db_conn, ins_queries, kwgs['max_workers'])

    log.info('Data reconsilation completed.')


def test_data_load_clear_transaction(db_conn, log, **kwgs):
    """ Load clear transaction to table. """
    
    # Create schema
    psql_execute(db_conn, 
                 kwgs['create_schm_query'].format(kwgs['schema_ods']))
    
    # Create table
    psql_execute(db_conn, 
             kwgs['create_fnl_tbl_query'].format(kwgs['schema_ods'],
                                                 kwgs['trg_table']))
    # Insert clean data to dds layer
    psql_execute(db_conn, 
             kwgs['final_query'].format(kwgs['schema_ods'], 
                                        kwgs['trg_table'],
                                        kwgs['schema_stg'],
                                        kwgs['res_table'],
                                        kwgs['tolerance']))
    
    log.info('Load clear transaction to table completed.')


if __name__ == "__main__":
  #Run reconsilation
    # Get parametes from file
    rec_dict = get_parameter('rec_variables.json')
    log = _logger('reconsilation')
    # Get connection creds
    dc = config(rec_dict['section'], rec_dict['config_file'])
    # Create test tables
    create_tables(dc, log, **rec_dict)
    # Populte data with tables and get list of partitions
    rec_dict['partitons'] = test_data_populating(dc, log, **rec_dict)
    # Get difference between 2 sources
    test_data_modification(dc, log, **rec_dict)
    # REconsilation process...
    test_data_reconsilation(dc, log, **rec_dict)
    # Load clear transactions to ods
    test_data_load_clear_transaction(dc, log, **rec_dict)
