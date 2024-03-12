"""
Entry point module for dat pipeline worker
"""
import os
import shlex
from subprocess import Popen, PIPE
from tempfile import NamedTemporaryFile
from celery import Celery
from dat_core.pydantic_models.connection import Connection
from dat_core.pydantic_models.dat_message import DatMessage, Type
from dat_core.pydantic_models.dat_log_message import DatLogMessage

TMP_DIR_LOCATION = '/tmp/.dat'
jobs_celery_app = Celery(broker='amqp://mq_user:mq_pass@message-queue:5672//')
telemetry_celery_app = Celery(
    broker='amqp://mq_user:mq_pass@message-queue:5672//')
os.makedirs(TMP_DIR_LOCATION, exist_ok=True)


def add_to_telemetry_q(msg: str) -> None:
    """Will add the passed message string to
    telemetry queue: dat-telemetry-q

    Args:
        msg (str): message to be added
    """
    telemetry_celery_app.send_task(
        'dat_telemetry_task', (msg, ), queue='dat-telemetry-q')


@jobs_celery_app.task(queue='dat-worker-q', name='dat_worker_task')
def worker(connection_str):
    '''celery worker
    Args:
        connection_obj (str)
    '''
    connection = Connection.model_validate_json(connection_str)
    # print(f'Received task with connection: {connection}')
    add_to_telemetry_q(
        msg=DatMessage(
        type=Type.LOG,
        log=DatLogMessage(
            message='Job run started',
            connection=connection.model_dump_json(),
        )).model_dump_json())
    with NamedTemporaryFile(mode='w', prefix='cnctn_src_',
                            dir=TMP_DIR_LOCATION) as src_tmp_file:
        src_tmp_file.write(connection_str)
        src_tmp_file.flush()
        _cmd = f'python src/Orchestrator/main.py -cfg {src_tmp_file.name}'
        with Popen(shlex.split(_cmd), stdout=PIPE) as proc:
            for line_a in proc.stdout:
                add_to_telemetry_q(msg=line_a.decode())

    add_to_telemetry_q(
        msg=DatMessage(
        type=Type.LOG,
        log=DatLogMessage(
            message='Job run ended',
            connection=connection.model_dump_json(),
        )).model_dump_json())

if __name__ == '__main__':
    # jobs_celery_app.send_task('dat_worker_task', (open(
    #     'connection.json').read(), ), queue='dat-worker-q')
    import json
    connection_str = open('connection.json').read()
    connection = Connection.model_validate_json(connection_str)
    add_to_telemetry_q(
        msg=DatMessage(
        type=Type.LOG,
        log=DatLogMessage(
            message='Connection run started',
            connection=connection.model_dump_json(),
        )).model_dump_json())
