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
