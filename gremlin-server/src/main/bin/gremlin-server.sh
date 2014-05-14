#!/bin/bash

case `uname` in
  CYGWIN*)
    CP="`dirname $0`"/../config/
    CP="$CP":$( echo `dirname $0`/../lib/*.jar . | sed 's/ /;/g')
    ;;
  *)
    CP="`dirname $0`"/../config/
    CP="$CP":$( echo `dirname $0`/../lib/*.jar . | sed 's/ /:/g')
esac
#echo $CP

# Find Java
if [ "$JAVA_HOME" = "" ] ; then
    JAVA="java -server"
else
    JAVA="$JAVA_HOME/bin/java -server"
fi

# Set Java options
if [ "$JAVA_OPTIONS" = "" ] ; then
    JAVA_OPTIONS="-Xms32m -Xmx512m"
fi

# Launch the application
$JAVA -Dlog4j.configuration=log4j-server.properties $JAVA_OPTIONS -cp $CP:$CLASSPATH com.tinkerpop.gremlin.server.GremlinServer $@

# Return the program's exit code
exit $?
