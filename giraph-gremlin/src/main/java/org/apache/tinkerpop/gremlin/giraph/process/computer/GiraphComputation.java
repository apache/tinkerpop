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

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.NullWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.ObjectWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.ComputerGraph;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GiraphComputation extends BasicComputation<ObjectWritable, VertexWritable, NullWritable, ObjectWritable> {

    public GiraphComputation() {
    }

    @Override
    public void compute(final Vertex<ObjectWritable, VertexWritable, NullWritable> vertex, final Iterable<ObjectWritable> messages) throws IOException {
        final GiraphWorkerContext workerContext = this.getWorkerContext();
        final VertexProgram<?> vertexProgram = workerContext.getVertexProgramPool().take();
        vertexProgram.execute(ComputerGraph.vertexProgram(vertex.getValue().get(), vertexProgram), workerContext.getMessenger((GiraphVertex) vertex, this, messages.iterator()), workerContext.getMemory());
        workerContext.getVertexProgramPool().offer(vertexProgram);
    }
}
