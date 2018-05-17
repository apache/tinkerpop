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
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.Configuring;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.util.PureTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class ConnectedComponentVertexProgramStep extends VertexProgramStep implements TraversalParent, Configuring {

    private Parameters parameters = new Parameters();
    private PureTraversal<Vertex, Edge> edgeTraversal;
    private String clusterProperty = ConnectedComponentVertexProgram.COMPONENT;

    public ConnectedComponentVertexProgramStep(final Traversal.Admin traversal) {
        super(traversal);
        this.configure(ConnectedComponent.edges, __.<Vertex>bothE());
    }

    @Override
    public void configure(final Object... keyValues) {
        if (keyValues[0].equals(ConnectedComponent.edges)) {
            if (!(keyValues[1] instanceof Traversal))
                throw new IllegalArgumentException("ConnectedComponent.edges requires a Traversal as its argument");
            this.edgeTraversal = new PureTraversal<>(((Traversal<Vertex,Edge>) keyValues[1]).asAdmin());
            this.integrateChild(this.edgeTraversal.get());
        } else if (keyValues[0].equals(ConnectedComponent.propertyName)) {
            if (!(keyValues[1] instanceof String))
                throw new IllegalArgumentException("ConnectedComponent.propertyName requires a String as its argument");
            this.clusterProperty = (String) keyValues[1];
        } else {
            this.parameters.set(this, keyValues);
        }
    }

    @Override
    public Parameters getParameters() {
        return parameters;
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.clusterProperty.hashCode();
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.clusterProperty, new GraphFilter(this.computer));
    }

    @Override
    public ConnectedComponentVertexProgram generateProgram(final Graph graph, final Memory memory) {
        final Traversal.Admin<Vertex, Edge> detachedTraversal = this.edgeTraversal.getPure();
        detachedTraversal.setStrategies(TraversalStrategies.GlobalCache.getStrategies(graph.getClass()));
        return ConnectedComponentVertexProgram.build().
                hasHalted(memory.exists(TraversalVertexProgram.HALTED_TRAVERSERS)).
                edges(detachedTraversal).
                property(this.clusterProperty).create(graph);
    }

    @Override
    public ConnectedComponentVertexProgramStep clone() {
        return (ConnectedComponentVertexProgramStep) super.clone();
    }

}
