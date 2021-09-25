FROM rapydo/backend:2.1
MAINTAINER "Mattia D'Antonio <m.dantonio@cineca.it>"

ARG RAPYDO_VERSION
RUN test -n "$RAPYDO_VERSION"

RUN apt-get update \
 && apt-get install -y zip unzip \
 && apt-get autoremove \
 && apt-get clean

RUN pip3 install --upgrade --no-cache-dir \
    git+https://github.com/EUDAT-B2STAGE/B2HANDLE.git@master \
    python-irodsclient==0.8.4 \
    gdapi-python==0.5.3
