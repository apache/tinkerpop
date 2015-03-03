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
package org.apache.tinkerpop.gremlin.hadoop.process.computer.giraph;

import org.apache.giraph.conf.GiraphConstants;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.HadoopGraphProvider;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoInputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoOutputFormat;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GiraphGraphProvider extends HadoopGraphProvider {

    @Override
    public Map<String, Object> getBaseConfiguration(final String graphName, final Class<?> test, final String testMethodName) {
        return new HashMap<String, Object>() {{
            put(Graph.GRAPH, HadoopGraph.class.getName());
            put(Constants.GREMLIN_HADOOP_GRAPH_INPUT_FORMAT, GryoInputFormat.class.getCanonicalName());
            put(Constants.GREMLIN_HADOOP_GRAPH_OUTPUT_FORMAT, GryoOutputFormat.class.getCanonicalName());
            put(Constants.GREMLIN_HADOOP_MEMORY_OUTPUT_FORMAT, SequenceFileOutputFormat.class.getCanonicalName());
            put(GiraphConstants.MIN_WORKERS, 1);
            put(GiraphConstants.MAX_WORKERS, 1);
            put(GiraphConstants.SPLIT_MASTER_WORKER.getKey(), false);
            //put("giraph.localTestMode", true);
            put(GiraphConstants.ZOOKEEPER_JAR, GiraphGraphComputer.class.getResource("zookeeper-3.3.3.jar").getPath());
            put("giraph.zkServerPort", "2181");  // you must have a local zookeeper running on this port
            put("giraph.nettyServerUseExecutionHandler", false); // this prevents so many integration tests running out of threads
            put("giraph.nettyClientUseExecutionHandler", false); // this prevents so many integration tests running out of threads
            put(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, "hadoop-gremlin/target/test-output");
            put(Constants.GREMLIN_HADOOP_DERIVE_MEMORY, true);
            put(Constants.GREMLIN_HADOOP_JARS_IN_DISTRIBUTED_CACHE, false);
        }};
    }
}
