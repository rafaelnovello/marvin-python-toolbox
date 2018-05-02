#!/usr/bin/env python
# coding=utf-8

# Copyright [2017] [B2W Digital]
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

try:
    import mock
except ImportError:
    import unittest.mock as mock

from marvin_python_toolbox.management import hive


@mock.patch('marvin_python_toolbox.management.hive.json')
def test_hive_generateconf_write_file_with_json(mocked_json):
    default_conf = [{
        "origin_host": "xxx_host_name",
        "origin_db": "xxx_db_name",
        "origin_queue": "marvin",
        "target_table_name": "xxx_table_name",
        "sample_sql": "SELECT * FROM XXX",
        "sql_id": "1"
    }]

    mocked_open = mock.mock_open()
    with mock.patch('marvin_python_toolbox.management.hive.open', mocked_open, create=True):
        hive.hive_generateconf(None)

    mocked_open.assert_called_once_with('hive_dataimport.conf', 'w')
    mocked_json.dump.assert_called_once_with(default_conf, mocked_open(), indent=2)


@mock.patch('marvin_python_toolbox.management.hive.HiveDataImporter.reset_remote_tables')
def test_hive_resetremote_call_HiveDataImporter_reset_remote_tables(reset_mocked): 
    hive.hive_resetremote(ctx=None, host="test", engine="test", queue="test")
    reset_mocked.assert_called_once_with()


@mock.patch('marvin_python_toolbox.management.hive.read_config')
@mock.patch('marvin_python_toolbox.management.hive.HiveDataImporter.__init__')
def test_hive_dataimport_without_config(init_mocked, read_config_mocked):
    read_config_mocked.return_value = None

    ctx = conf = sql_id = engine = \
        skip_remote_preparation = force_copy_files = validate = force =\
        force_remote = max_query_size = destination_host = destination_port =\
        destination_host_username = destination_host_password = destination_hdfs_root_path = None

    hive.hive_dataimport(
        ctx, conf, sql_id, engine, 
        skip_remote_preparation, force_copy_files, validate, force,
        force_remote, max_query_size, destination_host, destination_port,
        destination_host_username, destination_host_password, destination_hdfs_root_path
    )

    init_mocked.assert_not_called()


@mock.patch('marvin_python_toolbox.management.hive.read_config')
@mock.patch('marvin_python_toolbox.management.hive.HiveDataImporter.__init__')
@mock.patch('marvin_python_toolbox.management.hive.HiveDataImporter.table_exists')
@mock.patch('marvin_python_toolbox.management.hive.HiveDataImporter.import_sample')
def test_hive_dataimport_with_config(import_sample_mocked, table_exists_mocked, init_mocked, read_config_mocked):
    read_config_mocked.return_value = [{'origin_db': 'test', 'target_table_name': 'test'}]
    init_mocked.return_value = None

    ctx = sql_id = engine = \
        skip_remote_preparation = force_copy_files = validate =\
        force_remote = max_query_size = destination_port =\
        destination_host_username = destination_host_password = destination_hdfs_root_path = None

    force = True
    conf = '/path/to/conf'
    destination_host = 'test'

    hive.hive_dataimport(
        ctx, conf, sql_id, engine, 
        skip_remote_preparation, force_copy_files, validate, force,
        force_remote, max_query_size, destination_host, destination_port,
        destination_host_username, destination_host_password, destination_hdfs_root_path
    )

    init_mocked.assert_called_once_with(
        max_query_size=max_query_size,
        destination_host=destination_host,
        destination_port=destination_port,
        destination_host_username=destination_host_username,
        destination_host_password=destination_host_password,
        destination_hdfs_root_path=destination_hdfs_root_path,
        origin_db='test',
        target_table_name='test',
        engine=engine,
    )
    import_sample_mocked.assert_called_once_with(
        create_temp_table=True,
        copy_files=None,
        validate_query=None,
        force_create_remote_table=None
    )


@mock.patch('marvin_python_toolbox.management.hive.read_config')
@mock.patch('marvin_python_toolbox.management.hive.HiveDataImporter.__init__')
@mock.patch('marvin_python_toolbox.management.hive.HiveDataImporter.table_exists')
@mock.patch('marvin_python_toolbox.management.hive.HiveDataImporter.import_sample')
def test_hive_dataimport_with_config_sql_id(import_sample_mocked, table_exists_mocked, init_mocked, read_config_mocked):
    read_config_mocked.return_value = [
        {'origin_db': 'test', 'target_table_name': 'test', 'sql_id': 'test'},
        {'origin_db': 'bla', 'target_table_name': 'bla', 'sql_id': 'bla'},
    ]
    init_mocked.return_value = None

    ctx = sql_id = engine = \
        skip_remote_preparation = force_copy_files = validate =\
        force_remote = max_query_size = destination_port =\
        destination_host_username = destination_host_password = destination_hdfs_root_path = None

    sql_id= 'test'
    force = True
    conf = '/path/to/conf'
    destination_host = 'test'

    hive.hive_dataimport(
        ctx, conf, sql_id, engine, 
        skip_remote_preparation, force_copy_files, validate, force,
        force_remote, max_query_size, destination_host, destination_port,
        destination_host_username, destination_host_password, destination_hdfs_root_path
    )

    init_mocked.assert_called_once_with(
        max_query_size=max_query_size,
        destination_host=destination_host,
        destination_port=destination_port,
        destination_host_username=destination_host_username,
        destination_host_password=destination_host_password,
        destination_hdfs_root_path=destination_hdfs_root_path,
        origin_db='test',
        target_table_name='test',
        sql_id='test',
        engine=engine,
    )
    import_sample_mocked.assert_called_once_with(
        create_temp_table=True,
        copy_files=None,
        validate_query=None,
        force_create_remote_table=None
    )


@mock.patch('marvin_python_toolbox.management.hive.read_config')
@mock.patch('marvin_python_toolbox.management.hive.HiveDataImporter.table_exists')
@mock.patch('marvin_python_toolbox.management.hive.HiveDataImporter.import_sample')
def test_hive_dataimport_with_config_force_false(import_sample_mocked, table_exists_mocked, read_config_mocked):
    table_exists_mocked.return_value = False
    read_config_mocked.return_value = [{
        'origin_db': 'test',
        'target_table_name': 'test',
        'origin_queue':'test',
        'origin_host':'test',
        'sample_sql':'test',
        'sql_id':'test'
    }]

    ctx = sql_id = engine = \
        skip_remote_preparation = force_copy_files = validate =\
        force_remote = max_query_size = destination_port =\
        destination_host_username = destination_host_password = destination_hdfs_root_path = None

    force = False
    conf = '/path/to/conf'
    destination_host = 'test'

    hdi = hive.HiveDataImporter(
        max_query_size=max_query_size,
        destination_host=destination_host,
        destination_port=destination_port,
        destination_host_username=destination_host_username,
        destination_host_password=destination_host_password,
        destination_hdfs_root_path=destination_hdfs_root_path,
        origin_db='test',
        target_table_name='test',
        engine=engine,
        sql_id='test',
        origin_host='test',
        origin_queue='test',
        sample_sql='test',
    )

    with mock.patch.object(hive.HiveDataImporter, '__new__', return_value=hdi) as new_mocked:
        hive.hive_dataimport(
            ctx, conf, sql_id, engine, 
            skip_remote_preparation, force_copy_files, validate, force,
            force_remote, max_query_size, destination_host, destination_port,
            destination_host_username, destination_host_password, destination_hdfs_root_path
        )

        table_exists_mocked.assert_called_once_with(
            host=hdi.destination_host, db=hdi.origin_db, table=hdi.target_table_name
        )

        import_sample_mocked.assert_called_once_with(
            create_temp_table=True,
            copy_files=None,
            validate_query=None,
            force_create_remote_table=None
        )


@mock.patch('marvin_python_toolbox.management.hive.json')
@mock.patch('marvin_python_toolbox.management.hive.os.path')
def test_read_config_with_existing_path(path_mocked, json_mocked):
    path_mocked.exists.return_value = True
    path_mocked.join.return_value = 'test.conf'

    mocked_open = mock.mock_open()
    with mock.patch('marvin_python_toolbox.management.hive.open', mocked_open, create=True):
        hive.read_config("test.conf")

    mocked_open.assert_called_once_with('test.conf', 'r')
    json_mocked.load.assert_called_once_with(mocked_open())


@mock.patch('marvin_python_toolbox.management.hive.json')
@mock.patch('marvin_python_toolbox.management.hive.os.path')
def test_read_config_with_not_existing_path(path_mocked, json_mocked):
    path_mocked.exists.return_value = False
    path_mocked.join.return_value = 'test.conf'

    mocked_open = mock.mock_open()
    with mock.patch('marvin_python_toolbox.management.hive.open', mocked_open, create=True):
        hive.read_config("test.conf")

    mocked_open.assert_not_called()
    json_mocked.load.assert_not_called()


class TestHiveDataImporter:

    def setup_method(self, method):
        self.hdi = hive.HiveDataImporter(
            max_query_size=None,
            destination_host='test',
            destination_port=None,
            destination_host_username=None,
            destination_host_password=None,
            destination_hdfs_root_path='/tmp',
            origin_db='test',
            target_table_name='test',
            engine='test',
            sql_id='test',
            origin_host='test',
            origin_queue='test',
            sample_sql='test',
        )

        self.mock_methods = {
            'get_createtable_ddl': mock.DEFAULT,
            'get_partitions': mock.DEFAULT,
            'has_partitions': mock.DEFAULT,
            'create_database': mock.DEFAULT,
            'table_exists': mock.DEFAULT,
            'drop_table': mock.DEFAULT,
            'create_table': mock.DEFAULT,
            'populate_table': mock.DEFAULT,
            'get_table_location': mock.DEFAULT,
            'generate_table_location': mock.DEFAULT,
            'hdfs_dist_copy': mock.DEFAULT,
            'create_external_table': mock.DEFAULT,
            'refresh_partitions': mock.DEFAULT,
            'drop_view': mock.DEFAULT,
            'create_view': mock.DEFAULT,
            'validade_query': mock.DEFAULT,
            'get_connection': mock.DEFAULT,
            'print_finish_step': mock.DEFAULT,
            'print_start_step': mock.DEFAULT
        }

    @mock.patch('marvin_python_toolbox.management.hive.HiveDataImporter.count_rows')
    @mock.patch('marvin_python_toolbox.management.hive.HiveDataImporter.get_connection')
    @mock.patch('marvin_python_toolbox.management.hive.HiveDataImporter.retrieve_data_sample')
    def test_validade_query(self, retrieve_mocked, connection_mocked, count_rows_mocked):
        connection_mocked.return_value = 'connection_mocked'

        self.hdi.validade_query()

        connection_mocked.assert_called_once_with(
            host=self.hdi.origin_host, 
            db=self.hdi.origin_db, 
            queue=self.hdi.origin_queue
        )
        count_rows_mocked.assert_called_once_with(conn='connection_mocked', sql=self.hdi.sample_sql)
        retrieve_mocked.assert_called_once_with(conn='connection_mocked', full_table_name=self.hdi.full_table_name)

    @mock.patch('marvin_python_toolbox.management.hive.HiveDataImporter.show_log')
    @mock.patch('marvin_python_toolbox.management.hive.HiveDataImporter.get_connection')
    def test_table_exists_table_not_exists(self, connection_mocked, show_log_mocked):
        cursor = mock.MagicMock()
        conn = mock.MagicMock()
        conn.cursor.return_value = cursor
        cursor.fetchall.return_value = []
        connection_mocked.return_value = conn

        table_exists = self.hdi.table_exists(host='host', db='db', table='table')

        show_log_mocked.assert_called_once_with(cursor)
        assert table_exists is False

    @mock.patch('marvin_python_toolbox.management.hive.HiveDataImporter.show_log')
    @mock.patch('marvin_python_toolbox.management.hive.HiveDataImporter.get_connection')
    def test_table_exists_table_exists(self, connection_mocked, show_log_mocked):
        cursor = mock.MagicMock()
        conn = mock.MagicMock()
        conn.cursor.return_value = cursor
        cursor.fetchall.return_value = ['test']
        connection_mocked.return_value = conn

        table_exists = self.hdi.table_exists(host='host', db='db', table='table')

        show_log_mocked.assert_has_calls([mock.call(cursor)] * 2)
        assert table_exists is True

    @mock.patch('marvin_python_toolbox.management.hive.HiveDataImporter.show_log')
    @mock.patch('marvin_python_toolbox.management.hive.HiveDataImporter.drop_table')
    @mock.patch('marvin_python_toolbox.management.hive.HiveDataImporter.delete_files')
    @mock.patch('marvin_python_toolbox.management.hive.HiveDataImporter.get_connection')
    @mock.patch('marvin_python_toolbox.management.hive.HiveDataImporter._get_ssh_client')
    @mock.patch('marvin_python_toolbox.management.hive.HiveDataImporter.generate_table_location')
    def test_reset_remote_tables_without_valids_tables(self, tb_loc_mock, ssh_cli_mock, conn_mock, 
        delete_mock, drop_mock, log_mock):
        cursor = mock.MagicMock()
        conn = mock.MagicMock()
        conn.cursor.return_value = cursor
        cursor.fetchall.return_value = []
        conn_mock.return_value = conn

        self.hdi.reset_remote_tables()

        conn_mock.assert_called_once_with(
            host=self.hdi.origin_host, 
            db=self.hdi.temp_db_name, 
            queue=self.hdi.origin_queue
        )
        log_mock.assert_called_once_with(cursor)

        drop_mock.assert_not_called()
        tb_loc_mock.assert_not_called()
        delete_mock.assert_not_called()
        ssh_cli_mock.assert_not_called()

    @mock.patch('marvin_python_toolbox.management.hive.HiveDataImporter.show_log')
    @mock.patch('marvin_python_toolbox.management.hive.HiveDataImporter.drop_table')
    @mock.patch('marvin_python_toolbox.management.hive.HiveDataImporter.delete_files')
    @mock.patch('marvin_python_toolbox.management.hive.HiveDataImporter.get_connection')
    @mock.patch('marvin_python_toolbox.management.hive.HiveDataImporter._get_ssh_client')
    @mock.patch('marvin_python_toolbox.management.hive.HiveDataImporter.generate_table_location')
    def test_reset_remote_tables_with_valids_tables(self, tb_loc_mock, ssh_cli_mock, 
        conn_mock, delete_mock, drop_mock, log_mock):
        cursor = mock.MagicMock()
        conn = mock.MagicMock()
        conn.cursor.return_value = cursor
        cursor.fetchall.return_value = [['test']]
        conn_mock.return_value = conn

        tb_loc_mock.return_value = 'test'
        ssh_cli_mock.return_value = 'test'

        self.hdi.reset_remote_tables()

        conn_mock.assert_called_once_with(
            host=self.hdi.origin_host, 
            db=self.hdi.temp_db_name, 
            queue=self.hdi.origin_queue
        )
        log_mock.assert_called_once_with(cursor)

        drop_mock.assert_called_once_with(conn=conn, table_name="marvin.test")
        tb_loc_mock.assert_called_once_with(
            self.hdi.destination_hdfs_root_path,
            self.hdi.origin_host,
            self.hdi.temp_db_name + '.db',
            "test"
        )
        delete_mock.assert_called_once_with('test', 'test')
        ssh_cli_mock.assert_called_once_with(
            self.hdi.origin_host,
            self.hdi.destination_host_username,
            self.hdi.destination_host_password
        )

    @mock.patch('marvin_python_toolbox.management.hive.HiveDataImporter.validade_query')
    @mock.patch('marvin_python_toolbox.management.hive.HiveDataImporter.get_connection')
    @mock.patch('marvin_python_toolbox.management.hive.HiveDataImporter.print_finish_step')
    def test_import_sample_with_invalid_query_and_flag_true_stop(self, finish_step_mock, conn_mock, val_query_mock):
        val_query_mock.return_value = False

        self.hdi.import_sample(validate_query=True)

        val_query_mock.assert_called_once_with()
        finish_step_mock.assert_called_once_with()
        conn_mock.assert_not_called()

    @mock.patch('marvin_python_toolbox.management.hive.print')
    def test_import_sample_with_invalid_query_and_flag_false_dont_stop(self, print_mocked):
        with mock.patch.multiple('marvin_python_toolbox.management.hive.HiveDataImporter',
            **self.mock_methods
        ) as mocks:

            self.hdi.import_sample(validate_query=False)

            assert mocks['print_finish_step'].call_count == 6
            assert mocks['get_connection'].call_count == 5
            
            mocks['validade_query'].assert_not_called()

    @mock.patch('marvin_python_toolbox.management.hive.print')
    def test_import_sample_with_partitions_stop(self, print_mocked):
        with mock.patch.multiple('marvin_python_toolbox.management.hive.HiveDataImporter',
            **self.mock_methods
        ) as mocks:

            conn = mock.MagicMock()
            mocks['has_partitions'].return_value = True
            mocks['get_connection'].return_value = conn

            self.hdi.import_sample(validate_query=True)

            assert mocks['get_connection'].call_count == 2
            mocks['get_createtable_ddl'].assert_called_once_with(
                conn=conn,
                origin_table_name=self.hdi.target_table_name,
                dest_table_name=self.hdi.temp_table_name
            )
            mocks['get_partitions'].assert_called_once_with(
                mocks['get_createtable_ddl'].return_value
            )
            mocks['create_database'].assert_not_called()

    @mock.patch('marvin_python_toolbox.management.hive.print')
    def test_import_sample_with_create_temp_table_false_dont_call_create_table(self, print_mocked):
        with mock.patch.multiple('marvin_python_toolbox.management.hive.HiveDataImporter',
            **self.mock_methods
        ) as mocks:

            self.hdi.import_sample(create_temp_table=False)

            mocks['table_exists'].assert_not_called()
            mocks['drop_table'].assert_not_called()
            mocks['create_table'].assert_not_called()
            mocks['populate_table'].assert_not_called()

    @mock.patch('marvin_python_toolbox.management.hive.print')
    def test_import_sample_with_create_temp_table_true_call_create_table(self, print_mocked):
        with mock.patch.multiple('marvin_python_toolbox.management.hive.HiveDataImporter',
            **self.mock_methods
        ) as mocks:

            mocks['has_partitions'].return_value = False
            self.hdi.import_sample(create_temp_table=True, force_create_remote_table=True)

            assert mocks['drop_table'].call_count == 2
            assert mocks['create_table'].call_count == 1
            assert mocks['populate_table'].call_count == 1


    @mock.patch('marvin_python_toolbox.management.hive.print')
    def test_import_sample(self, print_mocked):
        with mock.patch.multiple('marvin_python_toolbox.management.hive.HiveDataImporter',
            **self.mock_methods
        ) as mocks:

            mocks['validade_query'].return_value = True
            mocks['has_partitions'].return_value = False
            self.hdi.import_sample()

            assert mocks['print_finish_step'].call_count == 6
            assert mocks['get_connection'].call_count == 5

    @mock.patch('marvin_python_toolbox.management.hive.HiveDataImporter.clean_ddl')
    def test_get_createtable_ddl(self, clean_ddl_mocked):
        cursor = mock.MagicMock()
        conn = mock.MagicMock()
        conn.cursor.return_value = cursor
        cursor.fetchall.return_value = [['l1'], ['l2']]
        dll = mock.MagicMock()
        clean_ddl_mocked.return_value = dll

        self.hdi.get_createtable_ddl(conn, 'marvin', 'test')

        cursor.execute.assert_called_once_with("SHOW CREATE TABLE marvin")
        clean_ddl_mocked.assert_called_once_with('l1l2', remove_formats=False, remove_general=True)
        dll.replace.assert_called_once_with('marvin', 'test')
        cursor.close.assert_called_once_with()

    @mock.patch('marvin_python_toolbox.management.hive.HiveDataImporter.show_log')
    def test_execute_db_command(self, show_log_mocked):
        cursor = mock.MagicMock()
        conn = mock.MagicMock()
        conn.cursor.return_value = cursor
        command = "bla bla bla"

        self.hdi._execute_db_command(conn, command)

        cursor.execute.assert_called_once_with(command)
        show_log_mocked.assert_called_once_with(cursor)
        cursor.close.assert_called_once_with()

    @mock.patch('marvin_python_toolbox.management.hive.hive')
    def test_get_connection(self, pyhive_mocked):
        host = 'test'
        self.hdi.get_connection(host, db='DEFAULT', queue='default')

        pyhive_mocked.connect.assert_called_once_with(
            host=host, database='DEFAULT',
            configuration={'mapred.job.queue.name': 'default',
                ' hive.exec.dynamic.partition.mode': 'nonstrict'}
        )

    @mock.patch('marvin_python_toolbox.management.hive.HiveDataImporter.show_log')
    def test_retrieve_data_sample(self, show_log_mocked):
        cursor = mock.MagicMock()
        conn = mock.MagicMock()
        conn.cursor.return_value = cursor
        cursor.description = [('table.col', 'type')]
        cursor.fetchall.return_value = ['test']

        full_table_name = 'test'
        sample_limit = 10

        data = self.hdi.retrieve_data_sample(conn, full_table_name, sample_limit)
        
        sql = "SELECT * FROM {} TABLESAMPLE ({} ROWS)".format(full_table_name, sample_limit)

        cursor.execute.assert_called_once_with(sql)
        assert data['data_header'][0]['col'] == 'col'
        assert data['data_header'][0]['table'] == 'table'
        assert data['data_header'][0]['type'] == 'type'
        assert data['total_lines'] == 1
        assert data['data'] == ['test']
        assert data['estimate_query_size'] == 104
        assert data['estimate_query_mean_per_line'] == 104
