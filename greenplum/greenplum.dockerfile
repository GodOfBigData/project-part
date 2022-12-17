FROM datagrip/greenplum:latest as builder

WORKDIR /

COPY run.sh /usr/local/bin/run.sh

RUN chmod a+x /usr/local/bin/run.sh

COPY setEnv.sh /usr/local/bin/setEnv.sh

RUN chmod a+x /usr/local/bin/setEnv.sh
