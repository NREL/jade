FROM python:3.7-slim as build

USER root

# update system
RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y jq vim git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /jade
COPY ./ /jade

RUN touch $HOME/.profile \
    && pip install -e . \
    # cleanup
    && rm -rf $HOME/.cache

# data volume
VOLUME data
WORKDIR /data

CMD [ "bash" ]