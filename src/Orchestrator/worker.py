"""
Entry point module for dat pipeline worker
"""
import os
import shlex
from subprocess import Popen, PIPE
from tempfile import NamedTemporaryFile
from celery import Celery
from dat_core.pydantic_models.connection import Connection

TMP_DIR_LOCATION = '/tmp/.dat'
app = Celery('tasks', broker='amqp://mq_user:mq_pass@message-queue:5672//')
os.makedirs(TMP_DIR_LOCATION, exist_ok=True)


@app.task(queue='dat-worker-q', name='dat_worker_task')
def worker(connection_str):
    '''celery worker
    Args:
        connection_obj (int): dc_admin.orchestrations.id
    '''
    connection = Connection.model_validate_json(connection_str)
    print(f'Received task with connection: {connection}')
    with NamedTemporaryFile(mode='w', prefix='cnctn_src_',
                            dir=TMP_DIR_LOCATION) as src_tmp_file:
        src_tmp_file.write(connection_str)
        src_tmp_file.flush()
        _cmd = f'python src/Orchestrator/main.py -cfg {src_tmp_file.name}'
        with Popen(shlex.split(_cmd), stdout=PIPE) as proc_in:
            for line_a in proc_in.stdout:
                # TODO: Add to another queue here
                print(f'foo: {line_a}')


if __name__ == '__main__':
    app.send_task('dat_worker_task', (open(
        'connection.json').read(), ), queue='dat-worker-q')
