############################################################
# Dockerfile to run an OrientDB (Graph) Container
# for current developed version
############################################################

FROM azul/zulu-openjdk:8

MAINTAINER ArcateAnalytics LTD (info@arcadeanalytics.com)

RUN mkdir /arcadedb

COPY arcadedb-*.tar.gz /arcadedb.tar.gz

RUN tar -xvzf arcadedb.tar.gz -C /arcadedb --strip-components=1\
      && rm arcadedb.tar.gz \
      && rm -rf /arcadedb/databases/*

RUN rm -rf /arcadedb/databases/*

ENV PATH /arcadedb/bin:$PATH

VOLUME [ "/arcadedb/databases"]

WORKDIR /arcadedb

EXPOSE 2424

EXPOSE 2480

# Default command start the server
CMD ["server.sh"]

