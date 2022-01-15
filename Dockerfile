FROM python:3.8-slim

ENV CONTAINER_HOME=/var/www

COPY . $CONTAINER_HOME
WORKDIR $CONTAINER_HOME

RUN pip install --upgrade pip \
    && pip install -r $CONTAINER_HOME/requirements.txt

CMD ["gunicorn","--log-level","DEBUG","--bind","0.0.0.0:443","main:app","--workers","4"]

