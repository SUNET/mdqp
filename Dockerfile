FROM python:3.10

ENV BASEDIR=/work

ENV PIP_INDEX_URL=https://pypi.sunet.se/simple

RUN python3 -m pip install poetry

COPY poetry.lock pyproject.toml /

RUN poetry config virtualenvs.create false \
  && poetry install --no-interaction --no-ansi

RUN mkdir -p $BASEDIR
COPY mdqp.py /mdqp.py
