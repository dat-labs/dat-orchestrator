FROM python:3.11
WORKDIR /repo
ARG SSH_PRIVATE_KEY
RUN mkdir /root/.ssh/ && echo "${SSH_PRIVATE_KEY}" >> /root/.ssh/id_rsa
RUN chmod 400 /root/.ssh/id_rsa && ssh-keyscan github.com >> /root/.ssh/known_hosts

COPY pyproject.toml .

RUN pip install -e .
# CMD python -m http.server
CMD celery -A src.Orchestrator.worker worker -Q dat-worker-q