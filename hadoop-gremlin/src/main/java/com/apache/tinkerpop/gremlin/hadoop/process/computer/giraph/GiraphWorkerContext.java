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
package com.apache.tinkerpop.gremlin.hadoop.process.computer.giraph;

import com.apache.tinkerpop.gremlin.hadoop.structure.io.ObjectWritable;
import com.apache.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import com.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import com.apache.tinkerpop.gremlin.process.computer.util.ImmutableMemory;
import org.apache.giraph.worker.WorkerContext;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GiraphWorkerContext extends WorkerContext {

    private VertexProgram<?> vertexProgram;
    private GiraphMemory memory;
    private GiraphMessenger messenger;

    public GiraphWorkerContext() {
        // Giraph ReflectionUtils requires this to be public at minimum
    }

    public void preApplication() throws InstantiationException, IllegalAccessException {
        this.vertexProgram = VertexProgram.createVertexProgram(ConfUtil.makeApacheConfiguration(this.getContext().getConfiguration()));
        this.memory = new GiraphMemory(this, this.vertexProgram);
        this.messenger = new GiraphMessenger();
    }

    public void postApplication() {

    }

    public void preSuperstep() {
        this.vertexProgram.workerIterationStart(new ImmutableMemory(this.memory));
    }

    public void postSuperstep() {
        this.vertexProgram.workerIterationEnd(new ImmutableMemory(this.memory));
    }

    public VertexProgram<?> getVertexProgram() {
        return this.vertexProgram;
    }

    public GiraphMemory getMemory() {
        return this.memory;
    }

    public GiraphMessenger getMessenger(final GiraphComputeVertex giraphComputeVertex, final Iterable<ObjectWritable> messages) {
        this.messenger.setCurrentVertex(giraphComputeVertex, messages);
        return this.messenger;
    }
}
