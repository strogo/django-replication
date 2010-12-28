import os
import logging
import logging.handlers
import sys
import traceback
import datetime	
import time
from multiprocessing import Process

from django.conf import settings
from django.db import connection as django_connection
from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render_to_response, get_object_or_404
from django.utils.importlib import import_module

from timelimited import TimeLimited, TimeLimitExpired
from models import Conduit, Log
from debug import debug

class DBHandler(logging.Handler):
    """Custom loggin handler that outputs to a hardcoded DB table"""
    def emit(self, record):
        ct = time.localtime(record.created)
        log_entry = Log(
            module = record.name,
            severity = record.levelname,
            message = record.msg
        ).save()


format="%(asctime)s |%(lineno)d |%(name)s |%(levelname)s | %(message)s"
dateformat="%Y-%m-%d %H:%M:%S" 
formatter = logging.Formatter(format, dateformat)

logger_ec = logging.getLogger('execute_conduit')
logger_es = logging.getLogger('execute_schedule')
logger_ec.setLevel(logging.DEBUG)
logger_es.setLevel(logging.DEBUG)

#filehandler = logging.handlers.RotatingFileHandler(getattr(settings, "REPLICATE_LOG_FILENAME", "/tmp/django_replicate.log"), maxBytes=20000, backupCount=4)
#filehandler.setFormatter(formatter)
#logger_ec.addHandler(filehandler)
#logger_es.addHandler(filehandler)

#Console
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
#logger_ec.addHandler(ch)
#logger_es.addHandler(ch)

dbhandler = DBHandler()
logger_ec.addHandler(dbhandler)
logger_es.addHandler(dbhandler)


def load_backend(backend_name):
    """Load the database backend; copy & pasted from Django"""
    try:
        # Most of the time, the database backend will be one of the official
        # backends that ships with Django, so look there first.
        return import_module('.base', 'django.db.backends.%s' % backend_name)
    except ImportError, e:
        # If the import failed, we might be looking for a database backend
        # distributed external to Django. So we'll try that next.
        try:
            return import_module('.base', backend_name)
        except ImportError, e_user:
            # The database backend wasn't found. Display a helpful error message
            # listing all possible (built-in) database backends.
            import django
            from django.core.exceptions import ImproperlyConfigured
            
            __path__ = django.__path__
            backend_dir = os.path.join(__path__[0], 'backends')
            try:
                available_backends = [f for f in os.listdir(backend_dir)
                        if os.path.isdir(os.path.join(backend_dir, f))
                        and not f.startswith('.')]
            except EnvironmentError:
                available_backends = []
            available_backends.sort()
            if backend_name not in available_backends:
                error_msg = "%r isn't an available database backend. Available options are: %s\nError was: %s" % \
                    (backend_name, ", ".join(map(repr, available_backends)), e_user)
                raise ImproperlyConfigured(error_msg)
            else:
                raise # If there's some other error, this must be an error in Django itself.

def execute_conduit_manually(conduit):
    """Helper view to manually execute a conduit"""
    try:
        logger_ec.info(u'Conduit: %s; Manually executing conduit...' % conduit)

        c_p = Process(target=execute_timed_conduit, args=(conduit,))
        c_p.start()	
#		execute_timed_conduit(conduit_obj)
        
    #	logger_ec.info('Conduit: %s; Finished manual execution.' % conduit_obj)
        
    except KeyboardInterrupt:
        debug("KeyboardInterrupt @ execute_conduit")
        
#http://code.activestate.com/recipes/137270/  by Christopher Prinos & others
def ResultIter(cursor, arraysize=1000, timeout = 0):
    """An iterator that uses fetchmany to keep memory usage down
    modified to add a timeout option"""
    try:		
        t_fetchmany = TimeLimited(cursor.fetchmany, timeout)
        while True:
            results = t_fetchmany(arraysize)
            if not results:
                break
            for result in results:
                yield result
    except KeyboardInterrupt:
        debug("KeyboardInterrupt @ ResultIter")
        return	

def run_timed_query(cursor, log_msg, query_string, timeout=10, *query_args):
    """Run a timed query, do error handling and logging"""
    try:
        result = TimeLimited(cursor.execute, timeout)(query_string, *query_args)
#		result = cursor.execute(query_string, *query_args)
        return result
    except TimeLimitExpired:
        logger_ec.error(u'%s; Timeout error.' % log_msg)
        raise
    except KeyboardInterrupt:
        #print "KeyboardInterrupt @ run_timed_query"
        return
    except:
        (exc_type, exc_info, tb) = sys.exc_info()
        logger_ec.error(u'%s; %s.' % (log_msg, traceback.format_exception(exc_type, exc_info, None)[0]))
        raise
        
def execute_timed_conduit(conduit_obj):
    django_connection.close() 

    try:
        return TimeLimited(execute_conduit, conduit_obj.timeout)(conduit_obj)
    except TimeLimitExpired:
        logger_ec.error(u'Conduit: %s; Conduit timeout error.' % conduit_obj)
        return sys.exc_info()
    except KeyboardInterrupt:
        #print "KeyboardInterrupt @ execute_timed_conduit"
        return	
    except:
        (exc_type, exc_info, tb) = sys.exc_info()
        logger_ec.error(u'Conduit: %s; Error; %s'  % (conduit_obj, traceback.format_exception(exc_type, exc_info, None)[0]))
        return traceback.format_exception(exc_type, exc_info, None)

def execute_conduit(conduit):
    """Execute a single conduit"""
    # rowcount is quirky for ORACLE 9
    #[7] The rowcount attribute may be coded in a way that updates
    #        its value dynamically. This can be useful for databases that
    #        return usable rowcount values only after the first call to
    #        a .fetch*() method.

    logger_ec.info(u"Conduit: %s; Started." % conduit)
    
    try:
        master_backend = load_backend(conduit.master_db.backend)
    except:
        (exc_type, exc_info, tb) = sys.exc_info()
        logger_ec.error(u'Conduit: %s; master_backend: %s; %s.' % (conduit, conduit.master_db.backend, traceback.format_exception(exc_type, exc_info, None)[0]))
        return traceback.format_exception(exc_type, exc_info, None)		
        
    if conduit.master_db.backend == 'oracle':
        #Reason unknown: Oracle 9 expects a str not an unicode
        master_connection = master_backend.DatabaseWrapper({
                'HOST': str(conduit.master_db.host.ip_address),
                'NAME': str(conduit.master_db.name),
                #TODO: turn conduit.master_db.options into a dict
                'OPTIONS': {},
                'USER': str(conduit.master_db.username),
                'PASSWORD': str(conduit.master_db.password),
                'PORT': str(conduit.master_db.port),
                'TIME_ZONE': str(conduit.master_db.timezone),
            })
    else:
        master_connection = master_backend.DatabaseWrapper({
            'HOST': conduit.master_db.host.ip_address,
            'NAME': conduit.master_db.name,
            #TODO: turn conduit.master_db.options into a dict
            'OPTIONS': {},
            'USER': conduit.master_db.username,
            'PASSWORD': conduit.master_db.password,
            'PORT': conduit.master_db.port,
            'TIME_ZONE': conduit.master_db.timezone,
        })
    try:
        master_cursor = TimeLimited(master_connection.cursor, conduit.minor_timeout)()
    except TimeLimitExpired:
        (exc_type, exc_info, tb) = sys.exc_info()
        logger_ec.error(u'Conduit: %s; master_db: %s; Timeout error.' % (conduit, conduit.master_db))
        return traceback.format_exception(exc_type, exc_info, None)
    except:
        (exc_type, exc_info, tb) = sys.exc_info()
        logger_ec.error(u'Conduit: %s; master_db: %s; %s.' % (conduit, conduit.master_db, traceback.format_exception(exc_type, exc_info, None)[0]))
        return traceback.format_exception(exc_type, exc_info, None)

    try:
        slave_backend = load_backend(conduit.slave_db.backend)
    except:
        (exc_type, exc_info, tb) = sys.exc_info()
        logger_ec.error(u'Conduit: %s; slave_backend: %s; %s.' % (conduit, conduit.slave_db.backend, traceback.format_exception(exc_type, exc_info, None)[0]))
        return traceback.format_exception(exc_type, exc_info, None)				
        
    slave_connection = slave_backend.DatabaseWrapper({
        'HOST': conduit.slave_db.host.ip_address,
        'NAME': conduit.slave_db.name,
        #TODO: turn conduit.master_db.options into a dict
        'OPTIONS': {},
        'USER': conduit.slave_db.username,
        'PASSWORD': conduit.slave_db.password,
        'PORT': conduit.slave_db.port,
        'TIME_ZONE': conduit.slave_db.timezone,
    })

    try:
        slave_cursor = TimeLimited(slave_connection.cursor, conduit.minor_timeout)()
    except TimeLimitExpired:
        (exc_type, exc_info, tb) = sys.exc_info()
        logger_ec.error(u'Conduit: %s; slave_db: %s; Timeout error.' % (conduit, conduit.slave_db))
        return traceback.format_exception(exc_type, exc_info, None)
    except:
        (exc_type, exc_info, tb) = sys.exc_info()
        logger_ec.error(u'Conduit: %s; slave_db: %s; %s.' % (conduit, conduit.slave_db, traceback.format_exception(exc_type, exc_info, None)[0]))
        return traceback.format_exception(exc_type, exc_info, None)

    # TODO: Implement also for other DBAs
    #ORACLE - SELECT B.COLUMN_NAME FROM ALL_CONSTRAINS A, ALL_CONS_COLUMNS B WHERE A.CONSTRAINT_NAME=B.CONTRAINT_NAME AND A.TABLE=<> AND A.CONTRAINT_TYPE='P';
    #ORACLE - SELECT * FROM ALL_CONSTRAINTS WHERE CONSTRAINT_TYPE='P' AND TABLE_NAME=<>
    #SQLITE - PRAGMA INDEX_LIST('<>');
    logger_ec.debug(u"Conduit: %s; slave_db: %s; conduit.detect_primary_key: %s" % (conduit, conduit.slave_db, conduit.detect_primary_key))
    logger_ec.debug(u"Conduit: %s; conduit.primary_key_source: %s" % (conduit, conduit.get_primary_key_source_display()))
    
    if conduit.primary_key_source == 'M':
        auto_db = conduit.master_db
        auto_backend = conduit.master_db.backend
        auto_cursor = master_cursor
        auto_table = conduit.master_table
    else:
        auto_db = conduit.slave_db
        auto_backend = conduit.slave_db.backend
        auto_cursor = slave_cursor
        auto_table = conduit.slave_table

    if conduit.detect_primary_key:
        if auto_backend == 'mysql':
            query =  "SHOW INDEX FROM %s WHERE Key_name = 'PRIMARY'" % auto_table
            error_msg = u'Conduit: %s; auto_db: %s; detecting_primary_key (mysql)' % (conduit, auto_db)
            try:
                run_timed_query(auto_cursor, error_msg, query, conduit.minor_timeout)
            except:
                slave_cursor.close()
                master_cursor.close()
                master_connection.close()
                slave_connection.close()
                return
        
            pk_column_names = [(k[4]) for k in auto_cursor.fetchall()]
        elif auto_backend == 'sqlite3':
            query =  "PRAGMA table_info ('%s')" % auto_table
            error_msg = u'Conduit: %s; auto_db: %s; detecting_primary_key (sqlite3)' % (conduit, auto_db)
            #Untested
            try:
                run_timed_query(auto_cursor, error_msg, query, conduit.minor_timeout)
            except:
                slave_cursor.close()
                master_cursor.close()
                master_connection.close()
                slave_connection.close()
                return
            
            #SQLITE format = cid | name | type | notnull | dflt_value | pk
            pk_column_names = [k[1] for k in auto_cursor.fetchall() if k[5]]
            """
            elif auto_backend == 'oracle':
    #			query = "SELECT B.COLUMN_NAME FROM ALL_CONSTRAINS A, ALL_CONS_COLUMNS B WHERE A.CONSTRAINT_NAME=B.CONTRAINT_NAME AND A.TABLE='%s' AND A.CONTRAINT_TYPE='P';" % auto_table
    #			query = "SELECT * FROM ALL_CONSTRAINTS WHERE CONSTRAINT_TYPE='P' AND VIEW_NAME LIKE 's%%'"# % auto_table

    #			query = u"
    #				SELECT l.column_name, l.position
    #				FROM all_constraints n
    #					JOIN all_cons_columns l ON l.owner = n.owner AND l.table_name = n.table_name AND l.constraint_name = n.constraint_name
    #					WHERE n.constraint_type = 'P' AND n.table_name = 'snap$_b1permit' AND n.owner LIKE '%%';
    #			"

    #			query = u"
    #				SELECT all_cons_columns.column_name, all_cons_columns.position
    #				FROM all_constraints n
    #					JOIN all_cons_columns l
    #						ON all_cons_columns.owner = all_constraints.owner
    #						AND all_cons_columns.table_name = all_constraints.table_name
    #						AND all_cons_columns.constraint_name = all_constraints.constraint_name
    #					WHERE
    #						all_constraints.constraint_type = 'P'
    #						AND all_constraints.table_name = 'snap$_b1permit'
    #						AND all_constraints.owner = 'SD'
    #			"

                print "QUERY: %s" % query
                error_msg = u'Conduit: %s; auto_db: %s; detecting_primary_key (oracle)' % (conduit, auto_db)
                try:
                    run_timed_query(auto_cursor, error_msg,  query, conduit.minor_timeout)
                except:
                    slave_cursor.close()
                    master_cursor.close()
                    master_connection.close()
                    slave_connection.close()
                    return
                pk_column_names = [k for k in auto_cursor.fetchall()]				
            """
        else:
            error_msg = u'Automatic primary key discovery is not yet supported this database backend.'
            logger_ec.error(u'Conduit: %s; auto_db: %s; %s' % (conduit, auto_db, error_msg))
            return
    else:
        pk_column_names = conduit.key_fields.split(',')

    #TODO: find correct way to determine if pk_column_name is empty
    if not pk_column_names or pk_column_names == ['']:
        error_msg = u'No primary key fields; check that your schema defines them, that automatic discover is supported for your database or provide them explicitly in the conduit.'
        logger_ec.error(u'Conduit: %s; auto_db: %s; %s' % (conduit, auto_db, error_msg))
        return True

    #Assemble query to fetch keys
    keys_query = "SELECT "+ ", ".join(["%s" % k for k in pk_column_names])
    keys_template = " AND ".join(["%s='%%s'" % k for k in pk_column_names])

    logger_ec.debug(u"Conduit: %s; keys_template: %s" % (conduit, keys_template))

    master_query = "%s FROM %s" % (keys_query, conduit.master_table)
    if conduit.master_subset:
        master_query += " WHERE %s" % conduit.master_subset

    slave_query = "%s FROM %s" % (keys_query, conduit.slave_table)
    if conduit.slave_subset:
        slave_query += " WHERE %s" % conduit.slave_subset

    logger_ec.debug(u'Conduit: %s; master_db: %s; master_query: %s' % (conduit, conduit.master_db, master_query))
    logger_ec.debug(u'Conduit: %s; slave_db: %s; slave_query: %s' % (conduit, conduit.slave_db, slave_query))

    #Assemble row fetching query
    if conduit.fields_to_fetch:
        fields_to_fetch = conduit.fields_to_fetch
    else:
        fields_to_fetch = '*'
    
    logger_ec.debug(u'Conduit: %s; fields_to_fetch: %s' % (conduit, fields_to_fetch))

    #TODO:
    #	- sqlite3

    try:
        if auto_backend == 'mysql' or auto_backend == 'postgresql' or auto_backend == 'postgresql_psycopg2':
            query = 'SELECT %s FROM %s LIMIT 1' % (fields_to_fetch, auto_table)
        elif auto_backend == 'oracle':
            query = 'SELECT %s FROM %s WHERE ROWNUM <= 1' % (fields_to_fetch, auto_table)
        elif auto_backend == 'ado_mssql':
            #UNTESTED
            query = 'SELECT TOP 1 % FROM %s' % (fields_to_fetch, auto_table)
        else:
            logger_ec.error(u'Conduit: %s; Automatic database field description is not yet supported this database backend: %s.' % (conduit, auto_backend))
            return True

        run_timed_query(auto_cursor, u'Conduit: %s; auto_db: %s; fields_to_fetch' % (conduit, auto_db),  query, conduit.minor_timeout)
    except:
        slave_cursor.close()
        master_cursor.close()
        master_connection.close()
        slave_connection.close()
        raise
        
    single_row = auto_cursor.fetchone()
    
    insert_template = ('INSERT INTO %s VALUES(%s)' % (conduit.slave_table, ", ".join(["%s" for k in auto_cursor.description])))

    fields_to_fetch = ', '.join([ k[0] for k in auto_cursor.description ])

    logger_ec.debug(u'Conduit: %s; insert_template: %s' % (conduit, insert_template))
    logger_ec.debug(u'Conduit: %s; converted fields_to_fetch: %s' % (conduit, fields_to_fetch))

    #EXECUTE KEY FETCH IN SLAVE
    logger_ec.debug(u'Conduit: %s; Starting fetching keys from slave....' % conduit)

    try:
        run_timed_query(slave_cursor, u'Conduit: %s; slave_db:%s; fetch_keys' % (conduit, conduit.slave_table), slave_query, conduit.major_timeout)
    except:
        slave_cursor.close()
        master_cursor.close()
        master_connection.close()
        slave_connection.close()
        return
        
    #TODO: convert to ResultIter
    slave_keys = slave_cursor.fetchall()
    logger_ec.debug(u'Conduit: %s; slave_table: %s; slave_keys: %s' % (conduit, conduit.slave_table, len(slave_keys)))

    slave_keys_dict = dict(zip(slave_keys, slave_keys))
    
    #EXECUTE KEY FETCH in MASTER
    logger_ec.debug(u'Conduit: %s; Starting fetching keys from master....' % conduit)

    try:
        run_timed_query(master_cursor, u'Conduit: %s; master_db: %s; fetch_keys' % (conduit, conduit.master_db), master_query, conduit.major_timeout)
    except:
        slave_cursor.close()
        master_cursor.close()
        master_connection.close()
        slave_connection.close()
        return

    logger_ec.debug(u'Conduit: %s; master_table: %s; master_key_buffersize: %s' % (conduit, conduit.master_table, conduit.master_key_batchsize))

    try:
        if conduit.master_key_batchsize:
            appendlist = [x for x in ResultIter(master_cursor, conduit.master_key_batchsize, conduit.major_timeout) if x not in slave_keys_dict]
        else:
            master_keys = TimeLimited(master_cursor.fetchall, conduit.major_timeout)()
            appendlist = [x for x in master_keys if x not in slave_keys_dict]
    except TimeLimitExpired:
        logger_ec.error(u'Conduit: %s; Fetching keys from master; Timeout error' % conduit)
        slave_cursor.close()
        master_cursor.close()
        master_connection.close()
        slave_connection.close()
        return 

    logger_ec.debug(u'Conduit: %s; master_table: %s; master_keys: %s' % (conduit, conduit.master_table, master_cursor.rowcount))
    logger_ec.info(u'Conduit: %s; Total rows to append: %s' % (conduit, len(appendlist)))

    batch_size = conduit.batchsize

    logger_ec.debug(u'Conduit: %s; batch_size: %s' % (conduit, batch_size))
    logger_ec.debug(u'Conduit: %s; dry_run: %s' % (conduit, conduit.dry_run))
    logger_ec.debug(u'Conduit: %s; Starting row fetch...' % (conduit))

    master_warning_counter = slave_warning_counter = row_fetch_count = 0

    #TODO: convert this loop's db query,etc to timelimited
    for key in appendlist[:batch_size]:
        #Fetch single row from master
        try:
            master_cursor.execute("SELECT %s FROM %s WHERE %s " % (fields_to_fetch, conduit.master_table, (keys_template) % (tuple(key))))
            #TODO: Implement multi-row fetch w/ cursor.executemany("SELECT ...", keys[])
        except:
            (exc_type, exc_info, tb) = sys.exc_info()
            logger_ec.error(u'Conduit: %s; master_table: %s; fetch_row: Database error: %s' % (conduit, conduit.master_table, exc_info))
            return
        
        #This is necesary to update rowcount
        row = master_cursor.fetchall()[0]
        #TODO: Implement multi-row fetch  w/ cursor.fetchmany(3) or w/ ResultIter
        if master_cursor.rowcount != 1:
            #TODO: rowcount != batch_block_size
            error_msg = 'Single master query returned an unexpected number or rows (0 or more than 1), check your primary keys.'
            if conduit.ignore_master_pull_errors:
                logger_ec.warning(u'master_table: %s; %s' % (conduit.master_table, error_msg))
                master_warning_counter += 1
                if master_warning_counter > conduit.master_warnings_abort_threshold and conduit.master_warnings_abort_threshold != 0:
                    error_msg = u'Master warning count threshold has been exceded.'
                    logger_ec.error(u'Conduit: %s; master_table: %s; %s' % (conduit, conduit.master_table, error_msg))
                    return
            else:
                logger_ec.error(u'Conduit: %s; master_table: %s; %s' % (conduit, conduit.master_table, error_msg))
                return

        #Insert row into slave
        try:
            if not conduit.dry_run:
                slave_cursor.execute(insert_template, row)
                #cursor.executemany("INSERT INTO animals (name, species) VALUES (%s, %s)", [  ('Rollo', 'Rat'),  ('Dudley', 'Dolphin'),  ('Mark', 'Marmoset') ])
                row_fetch_count += 1
        except: 
            (exc_type, exc_info, tb) = sys.exc_info()
            if conduit.ignore_slave_modify_errors:
                slave_warning_counter += 1
                logger_ec.warning(u'Conduit: %s; slave_table: %s; row_insert: Database error: %s' % (conduit, conduit.slave_table, exc_info))
                if slave_warning_counter > conduit.slave_warnings_abort_threshold and conduit.slave_warnings_abort_threshold != 0:
                    error_msg = u'Slave warning count threshold has been exceded.'
                    logger_ec.error(u'Conduit: %s; slave_table: %s; %s' % (conduit, conduit.slave_table, error_msg))
                    return
            else:
                logger_ec.error(u'Conduit: %s; slave_table: %s; row_insert: Database error: %s' % (conduit, conduit.slave_table, exc_info))
                return

    logger_ec.info(u'Conduit: %s; Total rows fetched from master: %d.' % (conduit, row_fetch_count))

    #Cleanup
    slave_cursor.close()
    master_cursor.close()
    #slave_connection.commit()
    master_connection.close()
    slave_connection.close()
    logger_ec.info(u'Conduit: %s; Finished.' % (conduit))

def execute_conduit_set(conduit_set):
    logger_es.info(u"Executing conduit_set: %s." % conduit_set)
    c_p_l = []
    for conduit in conduit_set.conduits.all():
        c_p = Process(target=execute_timed_conduit, args=(conduit,))
        c_p_l.append(c_p)
        c_p.start()
        #If conduit concurrency flag not set, wait for each conduit before starting next
        if not conduit_set.concurrent:
            c_p.join()
        
    for c_p in c_p_l:
        c_p.join()

    logger_es.info(u"Finished executing conduit_set: %s." % conduit_set)
    
def execute_schedule(schedule):
    """Execute a schedule, calling the associated conduit_set"""
    django_connection.close() 
    
    logger_es.info(u"Schedule: %s; Started." % schedule)

    schedule.executing = True
    schedule.save()

    execute_conduit_set(schedule.conduit_set)

    logger_es.info(u"Schedule: %s; Finished." % schedule)

    schedule.last_run = datetime.datetime.now()
    schedule.executing = False
    schedule.save()
