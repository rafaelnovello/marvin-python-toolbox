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
    # import pdb; pdb.set_trace()
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
