FROM rapydo/backend:2.3

ARG RAPYDO_VERSION
RUN test -n "$RAPYDO_VERSION"

RUN pip3 install --upgrade --no-cache-dir --quiet \
    git+https://github.com/EUDAT-B2STAGE/B2HANDLE.git@master \
    python-irodsclient==0.8.4 \
    gdapi-python==0.5.3
