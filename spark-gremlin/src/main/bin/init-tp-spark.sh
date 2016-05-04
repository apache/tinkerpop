#!/bin/bash
#
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

if [ -z $2 ]; then
  echo
  echo "Usage:"
  echo
  echo "  $0 SPARK_HOME SPARK_NODE_1 SPARK_NODE_2 ..."
  echo
  echo "Example:"
  echo
  echo "  $0 /usr/local/spark-1.6.1-bin-hadoop2.6 spark@master spark@slave1 spark@slave2 spark@slave3"
  echo
  exit 1
fi

if [ -z ${HADOOP_GREMLIN_LIBS} ]; then
  echo
  echo "HADOOP_GREMLIN_LIBS is not set; thus no jars to deploy."
  echo
  exit 1
fi

LOCAL_SPARK_GREMLIN_LIBS=$(tr ':' $'\n' <<< ${HADOOP_GREMLIN_LIBS} | grep 'spark-gremlin')
LIB_DIRNAME="hadoop-gremlin-libs"
TMP_DIR=/tmp/init-tp-spark
TMP_DIR2=${TMP_DIR}/${LIB_DIRNAME}

mkdir -p ${TMP_DIR2}
cp -R ${LOCAL_SPARK_GREMLIN_LIBS}/*.jar ${TMP_DIR2}

DIR=`dirname $0`

SPARK_HOME=${1}
SPARK_LIBS=${SPARK_HOME}/lib
SPARK_NODES=${@:2}

cat > ${TMP_DIR}/init-conf.sh <<EOF
#!/bin/bash

if [ ! -f "${SPARK_HOME}/conf/spark-env.sh" ]; then
  cp ${SPARK_HOME}/conf/spark-env.sh.template ${SPARK_HOME}/conf/spark-env.sh
fi
grep -F GREMLIN_LIBS ${SPARK_HOME}/conf/spark-env.sh > /dev/null || {
  echo >> ${SPARK_HOME}/conf/spark-env.sh
  echo "GREMLIN_LIBS=\\\$(find ${SPARK_LIBS}/${LIB_DIRNAME} -name '*.jar' | paste -sd ':')" >> ${SPARK_HOME}/conf/spark-env.sh
  echo "export SPARK_CLASSPATH=\\\${SPARK_CLASSPATH}:\\\${GREMLIN_LIBS}" >> ${SPARK_HOME}/conf/spark-env.sh
}
EOF

for node in ${SPARK_NODES}
do
  rsync -az --delete ${TMP_DIR2} ${node}:${SPARK_LIBS}
  ssh ${node} "bash -s" -- < ${TMP_DIR}/init-conf.sh
done

rm -rf ${TMP_DIR}
