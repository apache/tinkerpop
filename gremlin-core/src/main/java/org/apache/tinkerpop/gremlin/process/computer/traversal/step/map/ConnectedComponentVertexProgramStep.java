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

package org.apache.tinkerpop.gremlin.process.computer.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.computer.GraphFilter;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.clustering.connected.ConnectedComponentVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.clustering.peerpressure.PeerPressureVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class ConnectedComponentVertexProgramStep extends VertexProgramStep implements ByModulating {

    private String clusterProperty = ConnectedComponentVertexProgram.COMPONENT;

    public ConnectedComponentVertexProgramStep(final Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.clusterProperty.hashCode();
    }

    @Override
    public void modulateBy(final String clusterProperty) {
        this.clusterProperty = clusterProperty;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.clusterProperty, new GraphFilter(this.computer));
    }

    @Override
    public ConnectedComponentVertexProgram generateProgram(final Graph graph, final Memory memory) {
        return ConnectedComponentVertexProgram.build().
                hasHalted(memory.exists(TraversalVertexProgram.HALTED_TRAVERSERS)).
                property(this.clusterProperty).create(graph);
    }

    @Override
    public ConnectedComponentVertexProgramStep clone() {
        return (ConnectedComponentVertexProgramStep) super.clone();
    }
}
