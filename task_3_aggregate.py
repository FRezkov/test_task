#!/usr/bin/env python
# coding: utf-8

import concurrent.futures as cf
from modules.psqlModules import psql_execute, config
from modules.thread_tools import execute_concurent_process
from modules.utils import _logger, get_parameter, write_parameter


def create_agg_table(db_conn, log, param_file, **kwgs):
    '''Create schema and aggregate table'''
    
    if not kwgs.get('agg_table_exists'):
        
        # Create schema
        psql_execute(db_conn, 
                     kwgs['create_schm_query'].format(kwgs['schema_agg']))

        # Create tables
        psql_execute(db_conn, 
                     kwgs['create_agg_tbl_query'].format(kwgs['schema_agg'],
                                                     kwgs['trg_table']))

        # Create table's partitions
        for part in kwgs['partition_values']:
            psql_execute(db_conn, 
                         kwgs['create_tbl_partition'].format(
                                                    kwgs['schema_agg'],
                                                    kwgs['trg_table'],
                                                    part))
            # Create upsert function
            psql_execute(db_conn, 
                         kwgs['create_upsert_fnc'].format(kwgs['schema_agg'],
                                                         kwgs['trg_table']+
                                                         '_'+part.lower()))

            # Create upsert trigger
            psql_execute(db_conn, 
                         kwgs['create_upsert_trg'].format(kwgs['schema_agg'],
                                                          kwgs['trg_table']+
                                                          '_'+part.lower()))
        # Create primary key
        psql_execute(db_conn, 
                 kwgs['create_agg_tbl_pk'].format(kwgs['schema_agg'],
                                                 kwgs['trg_table'],
                                                 kwgs['pk']))

        # Create default partition tables
        psql_execute(db_conn, 
                     kwgs['create_tbl_def_partition'].format(kwgs['schema_agg'],
                                                     kwgs['trg_table']))

        # Create upsert function for default partition
        psql_execute(db_conn, 
                     kwgs['create_upsert_fnc'].format(kwgs['schema_agg'],
                                                     kwgs['trg_table']+
                                                     '_def'))

        # Create upsert trigger for default partition
        psql_execute(db_conn, 
                     kwgs['create_upsert_trg'].format(kwgs['schema_agg'],
                                                      kwgs['trg_table']+
                                                      '_def'))
        
        # Create indexes on source table 
        psql_execute(db_conn, 
                     kwgs['create_source_index'].format(kwgs['schema_ods'],
                                                        kwgs['src_table']))
        
        log.info('Schema and aggregate partition table are created.')
        
        kwgs['agg_table_exists'] = 'True'
        write_parameter(kwgs, param_file)
        
        log.info('Parameter file was updated.')
        
    else:
        log.info('Schema and aggregate partition table are already created before.')


def filling_agg_table(db_conn, log, **kwgs):
    '''Filling in the aggregate table'''
    
    # Prepare insert data 
    ins_queries = []
    for part in kwgs['partition_values']:
        ins_queries.append(
                 kwgs['agg_query'].format(kwgs['schema_agg'],
                                          kwgs['trg_table'],
                                          part,
                                          kwgs['schema_ods'],
                                          kwgs['src_table'],
                                          kwgs['load_period']))
        
    # Run multiinsert for agg table
    execute_concurent_process(db_conn, ins_queries, kwgs['max_workers'])
        
    log.info('Schema and aggregate partition table were created.')



if __name__ == "__main__":
  #Run aggregate data

    param_file = 'agg_variables.json'

    log = _logger('agg')
    # Get parameters from file
    agg_dict = get_parameter(param_file)
    # Get DB creds
    dc = config(agg_dict['section'], agg_dict['config_file'])
    # Create agg table and prepare source table
    create_agg_table(dc, log, param_file, **agg_dict)
    # Filling agg table with data
    filling_agg_table(dc, log, **agg_dict)



