"""
Copyright (c) 2017-present, Facebook, Inc.
All rights reserved.

This source code is licensed under the BSD-style license found in the
LICENSE file in the root directory of this source tree. An additional grant
of patent rights can be found in the PATENTS file in the same directory.
"""


from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals


get_lock = 'SELECT get_lock(%s, 0) as lockstatus'
release_lock = 'SELECT release_lock(%s) as lockstatus'
stop_slave_sql = "STOP SLAVE SQL_THREAD"
start_slave_sql = "START SLAVE SQL_THREAD"
kill_proc = "KILL %s"
start_transaction = "START TRANSACTION"
start_transaction_with_snapshot = "START TRANSACTION WITH CONSISTENT SNAPSHOT"
commit = 'COMMIT'
unlock_tables = 'UNLOCK TABLES'
show_processlist = "SHOW FULL PROCESSLIST"
show_slave_status = "SHOW SLAVE STATUS"
show_status = "SHOW STATUS LIKE %s "
select_max_statement_time = "SELECT MAX_STATEMENT_TIME=1000 1"
show_variables = "SHOW SESSION VARIABLES"

table_existence = (
    " SELECT 1 "
    " FROM information_schema.columns c1 "
    " WHERE c1.table_name = %s AND "
    "   c1.TABLE_SCHEMA = %s "
)

trigger_existence = (
    "SELECT TRIGGER_NAME, ACTION_TIMING, EVENT_MANIPULATION "
    "FROM INFORMATION_SCHEMA.TRIGGERS "
    "WHERE "
    "EVENT_OBJECT_TABLE = %s AND "
    "EVENT_OBJECT_SCHEMA = %s "
)

fetch_partition = (
    "SELECT PARTITION_NAME FROM INFORMATION_SCHEMA.PARTITIONS "
    "WHERE TABLE_SCHEMA = %s and TABLE_NAME = %s"
)

foreign_key_cnt = (
    "SELECT COUNT(*) AS count"
    "  FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS rc"
    "  JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS kcu"
    "   USING (CONSTRAINT_SCHEMA,CONSTRAINT_NAME)"
    " WHERE rc.REFERENCED_TABLE_NAME IS NOT NULL AND"
    " ("
    "   ( rc.TABLE_NAME = %s AND"
    "     rc.CONSTRAINT_SCHEMA = %s)"
    "   OR"
    "   ( rc.REFERENCED_TABLE_NAME = %s AND"
    "     rc.CONSTRAINT_SCHEMA = %s)"
    ")"
)

table_avg_row_len = (
    "SELECT AVG_ROW_LENGTH, TABLE_ROWS from "
    "information_schema.TABLES "
    "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s"
)

column_diff = (
    " SELECT c1.COLUMN_NAME "
    " FROM information_schema.columns c1 "
    " LEFT join information_schema.columns c2 ON "
    "   c1.COLUMN_NAME =  c2.COLUMN_NAME AND "
    "   c1.TABLE_SCHEMA = c2.TABLE_SCHEMA  AND "
    "   c2.table_name = %s "
    " WHERE c1.table_name = %s AND "
    "   c1.TABLE_SCHEMA = %s AND "
    "   c2.COLUMN_NAME IS NULL"
)


"""
Section for SQL components
Following functions only generates SQL components which can be a part of SQL.
"""


def escape(literal):
    """
    Escape the backtick in table/column name

    @param literal:  name string to escape
    @type  literal:  string

    @return:  escaped string
    @rtype :  string
    """
    return literal.replace('`', '``')


def list_to_col_str(column_list):
    """Basic helper function for turn a list of column names into a single
    string separated by comma, and escaping the column name in the meanwhile

    @param column_list:  list of column names
    @type  column_list:  list

    @return:  String of concated/escaped column names
    @rtype :  string
    """
    return ', '.join('`{}`'.format(escape(col)) for col in column_list)


def column_name_with_tbl_prefix(column_list, prefix):
    """Generate a comma seprated string, which attaches given prefix to the
    given list of column names.

    @param column_list:  list of column names
    @type  column_list:  list
    """
    return ', '.join(
        "`{}`.`{}`".format(escape(prefix), escape(col))
        for col in column_list
    )


def get_match_clause(left_table_name, right_table_name, columns, separator):
    """
    Given a left/right name and a list of columns, generate a where clause
    string for trigger creation/replaying changes
    For example if we have :
        left_table_name='OLD',
        right_table_name='NEW'
        columns=[{'name': foo}, {'name': bar}]
    this function will return following string:
        OLD.`foo` = NEW.`foo and OLD.`bar` = NEW.`bar`

    @param left_table_name: left table alias name
    @type  left_table_name: string
    @param right_table_name: right table alias name
    @type  right_table_name: string
    @param columns:  a list of columns to be appear in the where condition
    @type  columns:  list
    @return:  A raw string which is the where condition part of a Update
    SQL
    @rtype :  string
    """
    return separator.join(
        "`{}`.`{}` = `{}`.`{}`"
        .format(escape(left_table_name), escape(col),
                escape(right_table_name), escape(col))
        for col in columns)


def select_as(var_name, as_name):
    """
    SQL component for selecting column/variables with an alias
    """
    return "SELECT {} AS `{}`".format(var_name, escape(as_name))


def select_into(left_vars, right_vars):
    """
    SQL component for selecting left session variable's value into right one
    """

    return "SELECT {} INTO {}".format(left_vars, right_vars)


def assign_range_end_vars(columns, variables):
    """
    SQL component for passing session variables around when executing SELECT.
    This will generate list with elements like:
        @start_var:=`column_name`
    """
    if not columns:
        return ''
    assign_array = []
    for i in range(len(columns)):
        assign_array.append("{}:=`{}`".format(variables[i],
                                              escape(columns[i])))
    return assign_array


def wrap_checksum_function(column_to_wrap):
    """
    Wrap aggregation and checksum function outside column name.
    If we want to support customized checksum function, we will just need
    to do some changes here
    Columns should be escaped for backtick before passed in
    """
    return "bit_xor(crc32({}))".format(column_to_wrap)


def checksum_column_list(column_list):
    """
    Given a list of columna name, return a string of concated column names
    with checksum function wrapped
    """
    return ', '.join(
        wrap_checksum_function(i)
        for i in column_list
    )


def get_range_start_condition(columns, values):
    """
    Generate a where clase for chunk matching. The size of columns and values
    are equal.
    """
    condition_array = []
    for i in range(len(columns)):
        range_str = (
            "`{}` > {}"
            .format(columns[i], values[i]))
        if i > 0:
            prev_col = ' AND '.join(
                " `{}` = {} ".format(columns[i], values[i])
                for i in range(i)
            )
        else:
            prev_col = ''
        if prev_col:
            condition_array.append('( {} AND {} )'
                                   .format(range_str, prev_col))
        else:
            condition_array.append('( {} )'.format(range_str))
    return ' OR '.join(condition_array)


"""
Section for executable SQL
"""


def select_sql_no_fcache(table_name):
    return "SELECT SQL_NO_FCACHE * from `{}` limit 0".format(
        escape(table_name))


def show_create_table(table_name):
    return "SHOW CREATE TABLE `{}`".format(escape(table_name))


def show_table_stats(db_name):
    return (
        "SHOW TABLE STATUS IN `{}` LIKE %s"
        .format(escape(db_name)))


def create_delta_table(delta_table_name, id_col_name, dml_col_name,
                       mysql_engine, old_column_list, old_table_name):
    return (
        "CREATE TABLE `{}` "
        "(`{}` INT AUTO_INCREMENT, `{}` INT, PRIMARY KEY({}))"
        "ENGINE={} "
        "AS (SELECT {} FROM `{}` LIMIT 0)"
    ).format(
        escape(delta_table_name),
        escape(id_col_name), escape(dml_col_name),
        escape(id_col_name),
        mysql_engine,
        list_to_col_str(old_column_list),
        escape(old_table_name)
    )


def create_idx_on_delta_table(delta_table_name, pk_list):
    return (
        "CREATE INDEX `ix_pri` ON `{}` ({})"
    ).format(escape(delta_table_name), list_to_col_str(pk_list))


def create_insert_trigger(insert_trigger_name, table_name, delta_table_name,
                          dml_col_name, old_column_list, dml_type_insert):
    return (
        "CREATE TRIGGER `{}` AFTER INSERT ON `{}` FOR EACH ROW "
        "INSERT INTO `{}` ({}, {}) "
        "VALUES ({}, {})"
        .format(
            escape(insert_trigger_name),
            escape(table_name),
            escape(delta_table_name),
            escape(dml_col_name),
            list_to_col_str(old_column_list),
            dml_type_insert,
            column_name_with_tbl_prefix(old_column_list, 'NEW')))


def create_delete_trigger(delete_trigger_name, table_name, delta_table_name,
                          dml_col_name, old_column_list,
                          dml_type_delete):
    return (
        "CREATE TRIGGER `{}` AFTER DELETE ON `{}` FOR EACH ROW "
        "INSERT INTO `{}` ({}, {}) "
        "VALUES ({}, {})"
        .format(
            escape(delete_trigger_name),
            escape(table_name),
            escape(delta_table_name),
            escape(dml_col_name),
            list_to_col_str(old_column_list),
            dml_type_delete,
            column_name_with_tbl_prefix(old_column_list, 'OLD')))


def create_update_trigger(
        update_trigger_name, table_name,
        delta_table_name, dml_col_name, old_column_list,
        dml_type_update, dml_type_delete, dml_type_insert, pk_list):
    return (
        "CREATE TRIGGER `{}` AFTER UPDATE ON `{}` FOR EACH ROW "
        "IF ({}) THEN "
        "    insert into `{}` ({}, {}) "
        "    VALUES ({}, {}); "
        "ELSE "
        "    insert into `{}` ({}, {}) "
        "    VALUES ({}, {}), "
        "    ({}, {});"
        "END IF"
        .format(
            escape(update_trigger_name), escape(table_name),
            get_match_clause('OLD', 'NEW', pk_list, separator=' AND '),
            escape(delta_table_name), escape(dml_col_name),
            list_to_col_str(old_column_list),
            dml_type_update,
            column_name_with_tbl_prefix(old_column_list, 'NEW'),
            escape(delta_table_name), escape(dml_col_name),
            list_to_col_str(old_column_list),
            dml_type_delete,
            column_name_with_tbl_prefix(old_column_list, 'OLD'),
            dml_type_insert,
            column_name_with_tbl_prefix(old_column_list, 'NEW')))


def lock_tables(tables):
    lock_sql = 'LOCK TABLE '
    lock_sql += ', '.join(
        ['`{}` WRITE'.format(escape(tablename)) for tablename in tables])
    return lock_sql


def select_into_file(
        id_col_name, dml_col_name, delta_table_name, skip_fcache=False):
    if skip_fcache:
        no_fcache = "SQL_NO_FCACHE"
    else:
        no_fcache = ""
    return (
        "SELECT {} `{}`, `{}` "
        "FROM `{}` "
        "ORDER BY `{}` INTO OUTFILE %s"
        .format(no_fcache, escape(id_col_name), escape(dml_col_name),
                escape(delta_table_name), escape(id_col_name))
    )


def select_full_table_into_file(
        col_list, table_name, skip_fcache=False, where_filter=None):
    if skip_fcache:
        no_fcache = "SQL_NO_FCACHE"
    else:
        no_fcache = ""
    if where_filter:
        where_clause = "WHERE ({})".format(where_filter)
    else:
        where_clause = ""
    return (
        "SELECT {} {} "
        "FROM `{}` "
        "{} "
        "INTO OUTFILE %s"
    ).format(
        no_fcache,
        list_to_col_str(col_list),
        escape(table_name),
        where_clause)


def select_full_table_into_file_by_chunk(
        table_name, range_start_vars_array, range_end_vars_array,
        old_pk_list, old_non_pk_list, select_chunk_size, use_where,
        skip_fcache, where_filter, idx_name='PRIMARY'):
    assign = ', '.join(
        assign_range_end_vars(old_pk_list, range_end_vars_array))
    if use_where:
        row_range = get_range_start_condition(
            old_pk_list,
            range_start_vars_array)
        if where_filter:
            where_clause = " WHERE ({}) AND {} ".format(
                where_filter, row_range)
        else:
            where_clause = " WHERE {} ".format(row_range)
    else:
        if where_filter:
            where_clause = "WHERE ({}) ".format(where_filter)
        else:
            where_clause = ''

    if old_non_pk_list:
        column_name_list = '{}, {}'.format(
            assign, list_to_col_str(old_non_pk_list))
    else:
        column_name_list = assign

    if skip_fcache:
        no_fcache = "SQL_NO_FCACHE"
    else:
        no_fcache = ""

    return (
        "SELECT {} {} "
        "FROM `{}` FORCE INDEX (`{}`) {} "
        "ORDER BY {} LIMIT {} "
        "INTO OUTFILE %s "
        .format(no_fcache, column_name_list,
                escape(table_name), idx_name, where_clause,
                list_to_col_str(old_pk_list), select_chunk_size)
    )


def load_data_infile(table_name, col_list, ignore=False):
    ignore_str = 'IGNORE' if ignore else ''
    return (
        "LOAD DATA INFILE %s {} INTO TABLE `{}` "
        "CHARACTER SET BINARY ({})"
        .format(ignore_str, escape(table_name), list_to_col_str(col_list)))


def drop_index(idx_name, table_name):
    return "DROP INDEX `{}` ON `{}`".format(
        escape(idx_name), escape(table_name))


def insert_into_select_from(
        into_table, into_col_list, from_table, from_col_list):
    return (
        "INSERT INTO `{}` ({}) "
        "SELECT {} FROM `{}`"
    ).format(
        into_table,
        list_to_col_str(into_col_list),
        list_to_col_str(from_col_list),
        from_table)


def get_max_id_from(column, table_name):
    return (
        "SELECT ifnull(max(`{}`), 0) as max_id FROM `{}`"
        .format(escape(column), escape(table_name))
    )


def replay_delete_row(
        new_table_name, delta_table_name, id_col_name, pk_list):
    return (
        "DELETE {new} FROM `{new}`, `{delta}` WHERE "
        "`{delta}`.`{id_col}` IN %s AND {join_clause}"
    ).format(
        **{'new': escape(new_table_name),
           'delta': escape(delta_table_name),
           'id_col': escape(id_col_name),
           'join_clause': get_match_clause(
               new_table_name, delta_table_name,
               pk_list, separator=' AND ')})


def replay_insert_row(
        old_column_list, new_table_name, delta_table_name,
        id_col_name, ignore=False):
    ignore = "IGNORE" if ignore else ""
    return (
        "INSERT {ignore} INTO `{new}` ({cols})"
        "SELECT {cols} FROM `{delta}` WHERE "
        "`{delta}`.`{id_col}` IN %s "
    ).format(
        **{'ignore': ignore,
           'cols': list_to_col_str(old_column_list),
           'new': escape(new_table_name),
           'delta': escape(delta_table_name),
           'id_col': escape(id_col_name),
           })


def replay_update_row(
        old_non_pk_column_list, new_table_name, delta_table_name, ignore,
        id_col_name, pk_list):
    ignore = "IGNORE" if ignore else ""
    return (
        "UPDATE {ignore} `{new}`, `{delta}` "
        "SET {set} "
        "WHERE `{delta}`.`{id_col}` IN %s AND {join_clause} "
    ).format(
        **{'ignore': ignore,
           'new': escape(new_table_name),
           'delta': escape(delta_table_name),
           'set': get_match_clause(
               new_table_name, delta_table_name,
               old_non_pk_column_list, separator=', '),
           'id_col': escape(id_col_name),
           'join_clause': get_match_clause(
               new_table_name, delta_table_name,
               pk_list, separator=' AND ')})


def get_chg_row(id_col_name, dml_col_name, tmp_table_include_id):
    return (
        "SELECT `{id}`, `{dml_type}` "
        "FROM `{table}` "
        "WHERE `{id}` = %s "
    ).format(
        **{'id': escape(id_col_name),
           'dml_type': escape(dml_col_name),
           'table': escape(tmp_table_include_id)})


def get_replay_row_ids(
        id_col_name, dml_col_name, tmp_table_include_id, timeout_ms=None):
    if timeout_ms:
        statement_timeout_sql = "MAX_STATEMENT_TIME={}".format(timeout_ms)
    else:
        statement_timeout_sql = ''
    return (
        "SELECT {timeout} `{id}`, `{dml_type}` "
        "FROM `{table}` "
        "WHERE `{id}` > %s AND `{id}` <= %s "
        "ORDER BY `{id}`"
    ).format(
        **{'timeout': statement_timeout_sql,
           'id': escape(id_col_name),
           'dml_type': escape(dml_col_name),
           'table': escape(tmp_table_include_id)})


def drop_tmp_table(table_name):
    return "DROP TEMPORARY TABLE `{}`".format(escape(table_name))


def set_session_variable(variable):
    return (
        "SET SESSION {} = %s"
        .format(variable))


def add_index(table_name, indexes):
    """Generate sql to add indexes using ALTER TABLE

    @param param:  a list of indexes to create
    @type  param:  [sqlparse.models.TableIndex]

    @return:  sql to add indexes
    @rtype :  string

    """
    sql = "ALTER TABLE `{}` ".format(escape(table_name))
    idx_array = []
    for idx in indexes:
        idx_array.append("ADD {}".format(idx.to_sql()))
    # Execute alter table only if we have index to create
    if idx_array:
        sql += ', '.join(idx_array)
    return sql


def analyze_table(table_name):
    return "ANALYZE TABLE `{}`".format(escape(table_name))


def checksum_full_table(table_name, columns):
    """
    Generate SQL for checksuming data from given columns in table
    """
    checksum_sql = "SELECT count(*) as cnt, {} from `{}`"
    bit_xor_old_cols = [
        "bit_xor(crc32(`{}`))".format(escape(i.name))
        for i in columns
    ]
    checksum_sql = checksum_sql.format(
        ', '.join(bit_xor_old_cols), escape(table_name))
    return checksum_sql


def dump_current_chunk(
        table_name, columns, pk_list, range_start_values, chunk_size,
        force_index='PRIMARY', use_where=False):
    row_range = get_range_start_condition(
        pk_list,
        range_start_values
    )
    if use_where:
        where_clause = " WHERE {} ".format(row_range)
    else:
        where_clause = ""
    wrapped_pk_list = ', '.join(
        ['`{}`'.format(escape(col)) for col in pk_list])
    wrapped_non_pk = ', '.join(
        ['`{}`'.format(escape(col)) for col in columns])

    if wrapped_non_pk:
        column_name_list = '{}, {}'.format(
            wrapped_pk_list, wrapped_non_pk)
    else:
        column_name_list = wrapped_pk_list
    return (
        "SELECT {} FROM `{}` FORCE INDEX (`{}`) {} "
        "ORDER BY {} LIMIT {} INTO OUTFILE %s"
        .format(column_name_list,
                escape(table_name), escape(force_index), where_clause,
                list_to_col_str(pk_list), chunk_size)
    )


def checksum_by_chunk_with_assign(
        table_name, columns, pk_list,
        range_start_values, range_end_values, chunk_size, using_where,
        force_index='PRIMARY'):
    """
    Similar to checksum_by_chunk, this function has almost same the logic
    except: here we use original column name as checksum result column alias
    so that we can compare the result directly using list
    """
    if using_where:
        row_range = get_range_start_condition(
            pk_list,
            range_start_values
        )
        where_clause = " WHERE {} ".format(row_range)
    else:
        where_clause = ''
    assign = assign_range_end_vars(pk_list, range_end_values)
    # wrap all the column in checksum function
    bit_xor_assign_list = []
    for idx, assign_section in enumerate(assign):
        bit_xor_assign_list.append(
            wrap_checksum_function(assign_section) +
            ' AS `{}`'.format(escape(pk_list[idx]))
        )
    bit_xor_assign = ', '.join(bit_xor_assign_list)

    bit_xor_non_pk_list = [
        wrap_checksum_function('`{}`'.format(escape(col))) +
        ' AS `{}`'.format(escape(col))
        for col in columns
    ]
    bit_xor_non_pk = ', '.join(bit_xor_non_pk_list)

    if bit_xor_non_pk:
        column_name_list = '{}, {}'.format(
            bit_xor_assign, bit_xor_non_pk)
    else:
        column_name_list = bit_xor_assign

    return (
        "SELECT count(*) as _osc_chunk_cnt, {} "
        "FROM ( "
        " SELECT * FROM `{}` FORCE INDEX (`{}`) {} "
        "ORDER BY {} LIMIT {} ) as tmp"
        .format(column_name_list,
                escape(table_name), escape(force_index), where_clause,
                list_to_col_str(pk_list), chunk_size)
    )


def checksum_by_chunk(
        table_name, columns, pk_list,
        range_start_values, range_end_values, chunk_size, using_where,
        skip_fcache=False, force_index='PRIMARY'):
    if using_where:
        row_range = get_range_start_condition(
            pk_list,
            range_start_values
        )
        where_clause = " WHERE {} ".format(row_range)
    else:
        where_clause = ''
    assign = assign_range_end_vars(
        pk_list, range_end_values)
    # wrap all the column in checksum function
    bit_xor_assign = checksum_column_list(assign)
    bit_xor_non_pk = checksum_column_list(
        ['`{}`'.format(escape(col)) for col in columns])

    if bit_xor_non_pk:
        column_name_list = '{}, {}'.format(
            bit_xor_assign, bit_xor_non_pk)
    else:
        column_name_list = bit_xor_assign

    if skip_fcache:
        no_fcache = "SQL_NO_FCACHE"
    else:
        no_fcache = ""
    return (
        "SELECT {} count(*) as cnt, {} "
        "FROM ( "
        " SELECT * FROM `{}` FORCE INDEX (`{}`) {} "
        "ORDER BY {} LIMIT {} ) as tmp"
        .format(no_fcache, column_name_list,
                escape(table_name), escape(force_index), where_clause,
                list_to_col_str(pk_list), chunk_size)
    )


def checksum_by_replay_chunk(
        table_name, delta_table_name, old_column_list, pk_list, id_col_name,
        id_limit, max_replayed, chunk_size):
    col_list = ['count(*) AS `cnt`']
    for col in old_column_list:
        column_with_tbl = '`{}`.`{}`'.format(escape(table_name),
                                             escape(col))
        chksm = wrap_checksum_function(column_with_tbl)
        as_str = '{} AS `{}`'.format(chksm, escape(col))
        col_list.append(as_str)
    checksum_col_list = ', '.join(col_list)
    # We use not exists here to exclude changes which has been calculated
    # before to void duplicate efforts for hot row recreation
    return (
        "SELECT "
        "{col_list} "
        " FROM "
        " ( SELECT * FROM "
        "`{delta}` "
        "WHERE `{id}` > {id_limit} "
        "AND `{id}` <= "
        "least({id_limit} + {chunk_size}, {max_replayed}) "
        "AND NOT EXISTS ( "
        "SELECT 1 FROM `{delta}` as `t` WHERE {exist_join} "
        "AND `t`.{id} < `{delta}`.`{id}` )) as chg "
        "LEFT JOIN `{old_table}` "
        "ON {join_clause} "
    ).format(
        **{'id': escape(id_col_name),
           'col_list': checksum_col_list,
           'delta': escape(delta_table_name),
           'old_table': escape(table_name),
           'id_limit': id_limit,
           'max_replayed': max_replayed,
           'join_clause': get_match_clause(
               table_name, 'chg',
               pk_list, separator=' AND '),
           'exist_join': get_match_clause(
               "t", delta_table_name,
               pk_list, separator=' AND '),
           'chunk_size': chunk_size})


def rename_table(from_name, to_name):
    return (
        "ALTER TABLE `{}` rename `{}`"
        .format(escape(from_name),
                escape(to_name)))


def get_all_osc_tables(db=None):
    sql = (
        "SELECT table_schema as db, table_name "
        "FROM information_schema.tables "
        "WHERE left(table_name, length(%s)) = %s "
    )
    if db:
        sql += "AND table_schema = %s "
    return sql


def get_all_osc_triggers(db=None):
    sql = (
        "SELECT trigger_schema as db, trigger_name "
        "FROM information_schema.triggers "
        "WHERE left(trigger_name, length(%s)) = %s "
    )
    if db:
        sql += "AND trigger_schema = %s "
    return sql
