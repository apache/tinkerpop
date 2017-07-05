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
package org.apache.tinkerpop.gremlin.giraph.process.computer;

import org.apache.giraph.conf.GiraphConstants;
import org.apache.tinkerpop.gremlin.GraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.hadoop.HadoopGraphProvider;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@GraphProvider.Descriptor(computer = GiraphGraphComputer.class)
public final class GiraphHadoopGraphProvider extends HadoopGraphProvider {

    @Override
    public Map<String, Object> getBaseConfiguration(final String graphName, final Class<?> test, final String testMethodName, final LoadGraphWith.GraphData loadGraphWith) {
        final Map<String, Object> config = super.getBaseConfiguration(graphName, test, testMethodName, loadGraphWith);
        config.put("mapreduce.job.reduces", 2);
        /// giraph configuration
        config.put(GiraphConstants.LOCAL_TEST_MODE.getKey(), true); // local testing can only spawn one worker
        config.put(GiraphConstants.MIN_WORKERS, 1);
        config.put(GiraphConstants.MAX_WORKERS, 1);
        config.put(GiraphConstants.SPLIT_MASTER_WORKER.getKey(), false);
        config.put(GiraphConstants.ZOOKEEPER_IS_EXTERNAL.getKey(), false);
        config.put(GiraphConstants.NETTY_SERVER_USE_EXECUTION_HANDLER.getKey(), false); // this prevents so many integration tests running out of threads
        config.put(GiraphConstants.NETTY_CLIENT_USE_EXECUTION_HANDLER.getKey(), false); // this prevents so many integration tests running out of threads
        config.put(GiraphConstants.NETTY_USE_DIRECT_MEMORY.getKey(), true);
        config.put(GiraphConstants.NUM_INPUT_THREADS.getKey(), 2);
        config.put(GiraphConstants.NUM_COMPUTE_THREADS.getKey(), 2);
        config.put(GiraphConstants.MAX_MASTER_SUPERSTEP_WAIT_MSECS.getKey(), TimeUnit.MINUTES.toMillis(60L));
        config.put(GiraphConstants.VERTEX_OUTPUT_FORMAT_THREAD_SAFE.getKey(), false);
        config.put(GiraphConstants.NUM_OUTPUT_THREADS.getKey(), 1);
        return config;
    }

    @Override
    public GraphTraversalSource traversal(final Graph graph) {
        return graph.traversal().withComputer(GiraphGraphComputer.class);
    }

    @Override
    public GraphComputer getGraphComputer(final Graph graph) {
        return graph.compute(GiraphGraphComputer.class);
    }
}
