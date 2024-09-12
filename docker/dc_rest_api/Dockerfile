FROM ubuntu:22.04 AS build0

WORKDIR /usr/src/app

RUN apt-get update --fix-missing \
&& apt-get upgrade -y

RUN apt-get install -y \
  apt-utils \
  software-properties-common python3-dev \
  python3-setuptools \
  python3-pip \
  unixodbc unixodbc-dev \
  tdsodbc \
  git \
  locales \
  language-pack-en-base \
  language-pack-de-base


FROM build0 AS build_app

# the portal
RUN git clone -b main --single-branch https://github.com/ZFMK/DC_REST_API.git dc_rest_api && \
  cd dc_rest_api && \
  pip3 install -r requirements.txt && \
  python3 setup.py develop

FROM build_app AS run_app

ADD set_config.py ./dc_rest_api/set_config.py


ENTRYPOINT cd ./dc_rest_api && python3 ./set_config.py config.template.ini config.ini && pserve production.ini

CMD /usr/bin/bash

EXPOSE $PYRAMID_PORT
