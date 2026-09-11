<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
-->

Prerequisites
=============

Install the following prerequisites:

* Hadoop 2.7.2 installed [from Apache Hadoop download archive](http://archive.apache.org/dist/hadoop/common/hadoop-2.7.2/hadoop-2.7.2.tar.gz)
* Spark 2.4.3 installed [from Apache Spark project](https://spark.apache.org/downloads.html)


Set up HADOOP_GREMLIN_LIBS OS environment variable as of [TinkerPop Documentation](http://tinkerpop.apache.org/docs/current/reference/#hadoop-gremlin).

NOTE: HADOOP_GREMLIN_LIBS=*<hadoop installation dir>*/share/hadoop/common/lib seems good enough


Environment Variables Set
--------------------------------

The installation procedure should have set up the environment variables:
* HADOOP_HOME
* SPARK_HOME
* HADOOP_GREMLIN_LIBS
* PATH including the HADOOP_HOME and SPARK_HOME 


Verify the installed versions
-----------------------------

On the command line run:

    hadoop version
    spark-shell --version
