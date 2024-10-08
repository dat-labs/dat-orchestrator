import os
import json
import shlex
from multiprocessing import Process, Queue
from queue import Empty
from tempfile import NamedTemporaryFile
from subprocess import Popen, PIPE
import click
from typing import Mapping
from dat_core.pydantic_models.connection import Connection
import dat_client
from dat_client.configuration import Configuration
from dat_client.rest import ApiException


MAX_LEN_ROWS_BUFFER = 999
TMP_DIR_LOCATION = '/tmp/.dat'
_CMD_PREFIX = 'python src/Executables/main.py'
_CMD_GEN_MAP = {
    'source': {
        'cmd': 'read',
        'args': ['ctlg', 'cfg', 'cmb-state'],
    },
    'generator': {
        'cmd': 'generate',
        'args': ['cfg'],
    },
    'destination': {
        'cmd': 'write',
        'args': ['ctlg', 'cfg'],
    },
}


def _gen_cmd(actor_type: str, args: Mapping[str, str]) -> str:
    """Generate cmd line str.

    Args:
        actor_type (str): Actor type can be one of source, generator or destination
        args (Mapping): Mapping of args to their corresponding values

    Returns:
        str: cmd line str
    """
    _cmd_line_str = ''
    _cmd_line_str += _CMD_PREFIX
    _cmd_line_str += f" {_CMD_GEN_MAP[actor_type]['cmd']}"
    for arg in _CMD_GEN_MAP[actor_type]['args']:
        _cmd_line_str += ' ' + _gen_args(arg_value=args[arg], short_name=arg)
    return _cmd_line_str


def _gen_args(arg_value: str, short_name: str = None, long_name: str = None) -> str:
    """Generate arg string to be appended to a cmd. If both short_name and long_name
    are passed, short_name will be used.

    Args:
        arg_value (str): value of the argument
        short_name (str, optional): short name of the argument. Defaults to None.
        long_name (_type_, optional): long name of the argument. Defaults to None.

    Returns:
        str: _description_
    """
    _cmd = ''
    if long_name:
        _cmd += f'--{long_name}={arg_value}'
    if short_name:
        _cmd = f'-{short_name} {arg_value}'
    return _cmd


def gen_tmp_file(func):
    def wrapper(*args):
        connection_mdl = Connection.model_validate_json(args[0].read())
        with dat_client.ApiClient(Configuration(host="http://api:8000")) as api_client:
            conn_run_log_api_instance = dat_client.ConnectionRunLogsApi(
                api_client)
            src_state_response = {}
            try:
                src_state_response = conn_run_log_api_instance.get_combined_stream_states_connection_run_logs_connection_id_stream_states_get(
                    connection_id=connection_mdl.id,
                )
            except ApiException as e:
                print(
                    "Exception when calling ConnectionRunLogsApi->get_combined_stream_states_connection_run_logs_connection_id_stream_states_get: %s\n" % e)
        with NamedTemporaryFile(mode='w', prefix='cnctn_src_',
                                dir=TMP_DIR_LOCATION) as src_tmp_file:
            src_tmp_file.write(
                connection_mdl.source.model_dump_json())
            src_tmp_file.flush()
            with NamedTemporaryFile(mode='w', prefix='cnctn_src_',
                                    dir=TMP_DIR_LOCATION) as src_ctlg_tmp_file:
                src_ctlg_tmp_file.write(
                    connection_mdl.catalog.model_dump_json())
                src_ctlg_tmp_file.flush()
                with NamedTemporaryFile(mode='w', prefix='cnctn_src_',
                                    dir=TMP_DIR_LOCATION) as src_state_tmp_file:
                    src_state_tmp_file.write(json.dumps({_k: _v.model_dump(mode='json') for (_k, _v) in src_state_response.items()}))
                    src_state_tmp_file.flush()
                    with NamedTemporaryFile(mode='w', prefix='cnctn_gen_',
                                            dir=TMP_DIR_LOCATION) as gen_tmp_file:
                        gen_tmp_file.write(
                            connection_mdl.generator.model_dump_json())
                        gen_tmp_file.flush()
                        with NamedTemporaryFile(mode='w', prefix='cnctn_dst_',
                                                dir=TMP_DIR_LOCATION) as dst_tmp_file:
                            dst_tmp_file.write(
                                connection_mdl.destination.model_dump_json())
                            dst_tmp_file.flush()
                            with NamedTemporaryFile(mode='w', prefix='cnctn_dst_',
                                                    dir=TMP_DIR_LOCATION) as dst_ctlg_tmp_file:
                                dst_ctlg_tmp_file.write(
                                    connection_mdl.catalog.model_dump_json())
                                dst_ctlg_tmp_file.flush()
                                func(*args, src_state_tmp_file.name, src_tmp_file.name, src_ctlg_tmp_file.name,
                                    gen_tmp_file.name, dst_tmp_file.name, dst_ctlg_tmp_file.name)
    return wrapper


def src_cmd_proc(src_queue: Queue, src_cmd: str) -> None:
    with Popen(shlex.split(src_cmd), stdout=PIPE) as proc_in:
        for line_a in proc_in.stdout:
            src_queue.put(line_a)
    src_queue.put(None)


def gen_cmd_proc(src_queue: Queue, vectors_queue: Queue, gen_cmd: str) -> None:
    while True:
        item = src_queue.get()
        if item is None:
            break
        with Popen(shlex.split(gen_cmd), stdin=PIPE, stdout=PIPE) as proc_out:
            proc_out.stdin.write(item)
            vectors_queue.put(proc_out.communicate()[0])
    vectors_queue.put(None)


def dst_cmd_proc(vectors_queue: Queue, dst_cmd: str) -> None:
    def _process_items(items):
        with Popen(shlex.split(dst_cmd), stdin=PIPE, stdout=PIPE) as proc_out:
            for item in items:
                proc_out.stdin.write(item)
            print(proc_out.communicate()[0].decode(), flush=True)
    
    items = []
    while True:
        item = vectors_queue.get()
        if item is None:
            break

        items.append(item)
        if len(items) > MAX_LEN_ROWS_BUFFER:
            _process_items(items)
            items = []
    if not items:
        return
    _process_items(items)


@gen_tmp_file
def process(config, state_file, src_file, src_ctlg_file, gen_file, dst_file, dst_ctlg_file):
    src_cmd = _gen_cmd(
        actor_type='source',
        args={
            'ctlg': src_ctlg_file,
            'cfg': src_file,
            'cmb-state': state_file,
        }
    )
    gen_cmd = _gen_cmd(
        actor_type='generator',
        args={'cfg': gen_file, }
    )
    dst_cmd = _gen_cmd(
        actor_type='destination',
        args={'cfg': dst_file, 'ctlg': dst_ctlg_file, }
    )

    src_queue = Queue()
    vectors_queue = Queue()

    # start the source
    source_process = Process(target=src_cmd_proc, args=(src_queue, src_cmd, ))
    source_process.start()

    # start the generator
    generator_process = Process(
        target=gen_cmd_proc, args=(src_queue, vectors_queue, gen_cmd, ))
    generator_process.start()

    # start the destination
    destination_process = Process(
        target=dst_cmd_proc, args=(vectors_queue, dst_cmd))
    destination_process.start()

    # wait for all processes to finish
    source_process.join()
    generator_process.join()
    destination_process.join()

    # import pdb;pdb.set_trace()


@click.command
@click.option('--config', '-cfg', type=click.File(), required=True)
def cli(config):
    process(config)


if __name__ == '__main__':
    os.makedirs(TMP_DIR_LOCATION, exist_ok=True)
    cli()
    # foo('bar')
