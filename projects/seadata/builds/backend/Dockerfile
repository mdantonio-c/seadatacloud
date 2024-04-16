FROM rapydo/backend:2.4

ARG RAPYDO_VERSION
RUN test -n "$RAPYDO_VERSION"

RUN pip3 install --upgrade --no-cache-dir \
    gdapi-python==0.5.3 \
    bcrypt==4.0.1 \
    tzdata

