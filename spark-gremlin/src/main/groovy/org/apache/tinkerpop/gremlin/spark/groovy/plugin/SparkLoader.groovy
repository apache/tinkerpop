/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tinkerpop.gremlin.spark.groovy.plugin

import org.apache.spark.rdd.RDD
import org.apache.tinkerpop.gremlin.spark.structure.Spark
import scala.collection.JavaConversions

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class SparkLoader {

    public static void load() {

        Spark.metaClass.static.ls = {
            final List<String> rdds = new ArrayList<>();
            for (final RDD<?> rdd : Spark.getRDDs()) {
                rdds.add(rdd.name() + " [" + rdd.getStorageLevel().description() + "]")
            }
            return rdds;
        }

        Spark.metaClass.static.rm = { final String rddName ->
            for (final RDD<?> rdd : Spark.getRDDs()) {
                if (rdd.name().matches(rddName.replace(".", "\\.").replace("*", ".*")))
                    Spark.removeRDD(rdd.name());
            }
        }

        Spark.metaClass.static.head = { final String rddName ->
            return Spark.head(rddName, Integer.MAX_VALUE);
        }

        Spark.metaClass.static.head = { final String rddName, final int totalLines ->
            final List<Object> data = new ArrayList<>();
            final Iterator<?> itty = JavaConversions.asJavaIterator(Spark.getRDD(rddName).toLocalIterator());
            for (int i = 0; i < totalLines; i++) {
                if (itty.hasNext())
                    data.add(itty.next());
                else
                    break;
            }
            return data;
        }

        Spark.metaClass.static.describe = { final String rddName ->
            return Spark.getRDD(rddName).toDebugString();
        }
    }
}
