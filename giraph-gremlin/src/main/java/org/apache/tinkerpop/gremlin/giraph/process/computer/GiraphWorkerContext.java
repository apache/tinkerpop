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

import org.apache.commons.configuration.Configuration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.worker.WorkerContext;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.HadoopPools;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.ObjectWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.ImmutableMemory;
import org.apache.tinkerpop.gremlin.process.computer.util.VertexProgramPool;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.KryoShimServiceLoader;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GiraphWorkerContext extends WorkerContext {

    private VertexProgramPool vertexProgramPool;
    private GiraphMemory memory;

    public GiraphWorkerContext() {
        // Giraph ReflectionUtils requires this to be public at minimum
    }

    public void preApplication() throws InstantiationException, IllegalAccessException {
        final Configuration apacheConfiguration = ConfUtil.makeApacheConfiguration(this.getContext().getConfiguration());
        KryoShimServiceLoader.applyConfiguration(apacheConfiguration);
        final VertexProgram vertexProgram = VertexProgram.createVertexProgram(HadoopGraph.open(apacheConfiguration), apacheConfiguration);
        this.vertexProgramPool = new VertexProgramPool(vertexProgram, this.getContext().getConfiguration().getInt(GiraphConstants.NUM_COMPUTE_THREADS.getKey(), 1));
        this.memory = new GiraphMemory(this, vertexProgram);
    }

    public void postApplication() {

    }

    public void preSuperstep() {
        this.vertexProgramPool.workerIterationStart(new ImmutableMemory(this.memory));
    }

    public void postSuperstep() {
        this.vertexProgramPool.workerIterationEnd(new ImmutableMemory(this.memory));
    }

    public VertexProgramPool getVertexProgramPool() {
        return this.vertexProgramPool;
    }

    public GiraphMemory getMemory() {
        return this.memory;
    }

    public GiraphMessenger getMessenger(final GiraphVertex giraphVertex, final GiraphComputation giraphComputation, final Iterator<ObjectWritable> messages) {
        return new GiraphMessenger(giraphVertex, giraphComputation, messages);
    }
}
