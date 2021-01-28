#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

### BEGIN INIT INFO
# Provides:          gremlin-server
# Required-Start:    $remote_fs $syslog $network
# Required-Stop:     $remote_fs $syslog $network
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: Gremlin Server
# Description:       Apache Tinkerpop Gremlin Server
# chkconfig:         2345 98 01
### END INIT INFO

[[ -n "$DEBUG" ]] && set -x

SOURCE="$0"
while [[ -h "$SOURCE" ]]; do
  cd -P "$( dirname "$SOURCE" )" || exit 1
  DIR="$(pwd)"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
done
cd -P "$( dirname "$SOURCE" )" || exit 1
GREMLIN_BIN="$(pwd)"

GREMLIN_CONF=$GREMLIN_BIN/gremlin-server.conf

[[ -r $GREMLIN_CONF ]] && source $GREMLIN_CONF
[[ -n "$DEBUG" ]] && set -x

if [[ -z "$GREMLIN_HOME" ]]; then
  cd ..
  GREMLIN_HOME="$(pwd)"
fi

if [[ -z "$LOG_DIR" ]] ; then
  LOG_DIR="$GREMLIN_HOME/logs"
fi

if [[ -z "$LOG_FILE" ]]; then
  LOG_FILE="$LOG_DIR/gremlin.log"
fi

if [[ -z "$PID_DIR" ]] ; then
  PID_DIR="$GREMLIN_HOME/run"
fi

if [[ -z "$PID_FILE" ]]; then
  PID_FILE=$PID_DIR/gremlin.pid
fi

if [[ -z "$GREMLIN_YAML" ]]; then
  GREMLIN_YAML=$GREMLIN_HOME/conf/gremlin-server.yaml
fi

if [[ ! -r "$GREMLIN_YAML" ]]; then
  # try relative to home
  GREMLIN_YAML="$GREMLIN_HOME/$GREMLIN_YAML"
  if [[ ! -r "$GREMLIN_YAML" ]]; then
    echo WARNING: $GREMLIN_YAML is unreadable
  fi
fi

# absolute file path requires 'file:'
LOG4J_CONF="file:$GREMLIN_HOME/conf/log4j-server.properties"

# Find Java
if [[ "$JAVA_HOME" = "" ]] ; then
    JAVA="java"
else
    JAVA="$JAVA_HOME/bin/java"
fi

# Set Java options
if [[ "$JAVA_OPTIONS" = "" ]] ; then
    JAVA_OPTIONS="-Xms512m -Xmx4096m"
fi

# Build Java CLASSPATH
CP="$GREMLIN_HOME/conf/"
CP="$CP":$( echo $GREMLIN_HOME/lib/*.jar . | sed 's/ /:/g')
CP="$CP":$( find -L "$GREMLIN_HOME"/ext -mindepth 1 -maxdepth 1 -type d | \
        sort | sed 's/$/\/plugin\/*/' | tr '\n' ':' )

CLASSPATH="${CLASSPATH:-}:$CP"

GREMLIN_SERVER_CMD=org.apache.tinkerpop.gremlin.server.GremlinServer
GREMLIN_INSTALL_CMD=org.apache.tinkerpop.gremlin.server.util.GremlinServerInstall


isRunning() {
  if [[ -r "$PID_FILE" ]] ; then
    PID=$(cat "$PID_FILE")
    ps -p "$PID" &> /dev/null
    return $?
  else
    return 1
  fi
}

status() {
  isRunning
  RUNNING=$?
    if [[ $RUNNING -gt 0 ]]; then
      echo Server not running
    else
      echo Server running with PID $(cat "$PID_FILE")
    fi
}

stop() {
  isRunning
  RUNNING=$?
  if [[ $RUNNING -gt 0 ]]; then
    echo Server not running
    rm -f "$PID_FILE"
  else
    kill "$PID" &> /dev/null || { echo "Unable to kill server [$PID]"; exit 1; }
    for i in $(seq 1 60); do
      ps -p "$PID" &> /dev/null || { echo "Server stopped [$PID]"; rm -f "$PID_FILE"; return 0; }
      [[ $i -eq 30 ]] && kill "$PID" &> /dev/null
      sleep 1
    done
    echo "Unable to kill server [$PID]";
    exit 1;
  fi
}

start() {
  isRunning
  RUNNING=$?
  if [[ $RUNNING -eq 0 ]]; then
    echo Server already running with PID $(cat "$PID_FILE").
    exit 1
  fi

  if [[ -z "$RUNAS" ]]; then

    mkdir -p "$LOG_DIR" &>/dev/null
    if [[ ! -d "$LOG_DIR" ]]; then
      echo ERROR: LOG_DIR $LOG_DIR does not exist and could not be created.
      exit 1
    fi

    mkdir -p "$PID_DIR" &>/dev/null
    if [[ ! -d "$PID_DIR" ]]; then
      echo ERROR: PID_DIR $PID_DIR does not exist and could not be created.
      exit 1
    fi

    $JAVA -Dlog4j.configuration=$LOG4J_CONF $JAVA_OPTIONS -cp $CLASSPATH $GREMLIN_SERVER_CMD "$GREMLIN_YAML" >> "$LOG_FILE" 2>&1 &
    PID=$!
    disown $PID
    echo $PID > "$PID_FILE"
  else

    su -c "mkdir -p $LOG_DIR &>/dev/null"  "$RUNAS"
    if [[ ! -d "$LOG_DIR" ]]; then
      echo ERROR: LOG_DIR $LOG_DIR does not exist and could not be created.
      exit 1
    fi

    su -c "mkdir -p $PID_DIR &>/dev/null"  "$RUNAS"
    if [[ ! -d "$PID_DIR" ]]; then
      echo ERROR: PID_DIR $PID_DIR does not exist and could not be created.
      exit 1
    fi

    su -c "$JAVA -Dlog4j.configuration=$LOG4J_CONF $JAVA_OPTIONS -cp $CLASSPATH $GREMLIN_SERVER_CMD \"$GREMLIN_YAML\" >> \"$LOG_FILE\" 2>&1 & echo \$! "  "$RUNAS" > "$PID_FILE"
    chown "$RUNAS" "$PID_FILE"
  fi

  isRunning
  RUNNING=$?
  if [[ $RUNNING -eq 0 ]]; then
    echo Server started $(cat "$PID_FILE").
    exit 0
  else
    echo Server failed
    exit 1
  fi

}

startForeground() {
  isRunning
  RUNNING=$?
  if [[ $RUNNING -eq 0 ]]; then
    echo Server already running with PID $(cat "$PID_FILE").
    exit 1
  fi

  if [[ -z "$RUNAS" ]]; then
    $JAVA -Dlog4j.configuration=$LOG4J_CONF $JAVA_OPTIONS -cp $CLASSPATH $GREMLIN_SERVER_CMD "$GREMLIN_YAML"
    exit 0
  else
    echo Starting in foreground not supported with RUNAS
    exit 1
  fi

}

install() {

  isRunning
  RUNNING=$?
  if [[ $RUNNING -eq 0 ]]; then
    echo Server must be stopped before installing.
    exit 1
  fi

  echo Installing dependency $@

  DEPS="$@"
  if [[ -z "$RUNAS" ]]; then
    $JAVA -Dlog4j.configuration=$LOG4J_CONF $JAVA_OPTIONS -cp $CLASSPATH $GREMLIN_INSTALL_CMD $DEPS
  else
    su -c "$JAVA -Dlog4j.configuration=$LOG4J_CONF $JAVA_OPTIONS -cp $CLASSPATH $GREMLIN_INSTALL_CMD $DEPS "  "$RUNAS"
  fi

}

case "$1" in
  status)
    status
    ;;
  restart)
    stop
    start
    ;;
  start)
    start
    ;;
  stop)
    stop
    ;;
  install)
    shift
    install "$@"
    ;;
  console)
    startForeground
    ;;
  *)
    if [[ -n "$1" ]] ; then
      if [[ -r "$1" ]]; then
        GREMLIN_YAML="$1"
        startForeground
      elif [[ -r "$GREMLIN_HOME/$1" ]] ; then
        GREMLIN_YAML="$GREMLIN_HOME/$1"
        startForeground
      fi
      echo Configuration file not found.
    fi
    echo "Usage: $0 {start|stop|restart|status|console|install <group> <artifact> <version>|<conf file>}"
    echo
    echo "    start        Start the server in the background using conf/gremlin-server.yaml as the"
    echo "                 default configuration file"
    echo "    stop         Stop the server"
    echo "    restart      Stop and start the server"
    echo "    status       Check if the server is running"
    echo "    console      Start the server in the foreground using conf/gremlin-server.yaml as the"
    echo "                 default configuration file"
    echo "    install      Install dependencies"
    echo
    echo "If using a custom YAML configuration file then specify it as the only argument for Gremlin"
    echo "Server to run in the foreground or specify it via the GREMLIN_YAML environment variable."
    echo
    exit 1
    ;;
esac
