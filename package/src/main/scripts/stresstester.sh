#!/usr/bin/env bash
# Copyright (c) Arcade Analytics LTD (https://www.arcadeanalytics.com)

# resolve links - $0 may be a softlink
PRG="$0"

while [ -h "$PRG" ]; do
  ls=`ls -ld "$PRG"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    PRG=`dirname "$PRG"`/"$link"
  fi
done

# Get standard environment variables
PRGDIR=`dirname "$PRG"`

# Only set ARCADEDB_HOME if not already set
[ -f "$ARCADEDB_HOME"/lib/arcadedb-console-@VERSION@.jar ] || ARCADEDB_HOME=`cd "$PRGDIR/.." ; pwd`
export ARCADEDB_HOME


# Set JavaHome if it exists
if [ -f "${JAVA_HOME}/bin/java" ]; then
   JAVA=${JAVA_HOME}/bin/java
else
   JAVA=java
fi
export JAVA

exec "$JAVA" -cp "$ARCADEDB_HOME/lib/*" com.arcadedb.stresstest.StressTester $*
