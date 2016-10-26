/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.spark.process.computer;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.spark.structure.Spark;
import org.apache.tinkerpop.gremlin.spark.structure.io.gryo.GryoSerializer;

import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SparkHadoopGraphGryoSerializerProvider extends SparkHadoopGraphProvider {

    public Map<String, Object> getBaseConfiguration(final String graphName, final Class<?> test, final String testMethodName, final LoadGraphWith.GraphData loadGraphWith) {
        if (!SparkHadoopGraphGryoSerializerProvider.class.getCanonicalName().equals(System.getProperty(PREVIOUS_SPARK_PROVIDER, null))) {
            Spark.close();
            System.setProperty(PREVIOUS_SPARK_PROVIDER, SparkHadoopGraphGryoSerializerProvider.class.getCanonicalName());
        }
        final Map<String, Object> config = super.getBaseConfiguration(graphName, test, testMethodName, loadGraphWith);
        config.put(Constants.SPARK_SERIALIZER, GryoSerializer.class.getCanonicalName());
        config.remove(Constants.SPARK_KRYO_REGISTRATOR);
        return config;
    }
}
