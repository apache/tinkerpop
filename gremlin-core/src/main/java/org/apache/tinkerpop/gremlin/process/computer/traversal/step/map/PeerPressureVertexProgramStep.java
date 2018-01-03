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
import org.apache.tinkerpop.gremlin.process.computer.clustering.peerpressure.PeerPressureVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.lambda.HaltedTraversersCountTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.TimesModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.PureTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class PeerPressureVertexProgramStep extends VertexProgramStep implements TraversalParent, ByModulating, TimesModulating {

    private PureTraversal<Vertex, Edge> edgeTraversal;
    private String clusterProperty = PeerPressureVertexProgram.CLUSTER;
    private int times = 30;

    public PeerPressureVertexProgramStep(final Traversal.Admin traversal) {
        super(traversal);
        this.modulateBy(__.<Vertex>outE().asAdmin());
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.edgeTraversal.hashCode() ^ this.clusterProperty.hashCode() ^ this.times;
    }

    @Override
    public void modulateBy(final Traversal.Admin<?, ?> edgeTraversal) {
        this.edgeTraversal = new PureTraversal<>((Traversal.Admin<Vertex, Edge>) edgeTraversal);
        this.integrateChild(this.edgeTraversal.get());
    }

    @Override
    public void modulateBy(final String clusterProperty) {
        this.clusterProperty = clusterProperty;
    }

    @Override
    public void modulateTimes(int times) {
        this.times = times;
    }

    @Override
    public List<Traversal.Admin<Vertex, Edge>> getLocalChildren() {
        return Collections.singletonList(this.edgeTraversal.get());
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.edgeTraversal.get(), this.clusterProperty, this.times, new GraphFilter(this.computer));
    }

    @Override
    public PeerPressureVertexProgram generateProgram(final Memory memory, final Graph... graphs) {
        final Traversal.Admin<Vertex, Edge> detachedTraversal = this.edgeTraversal.getPure();
        detachedTraversal.setStrategies(TraversalStrategies.GlobalCache.getStrategies(graphs[0].getClass()));
        final PeerPressureVertexProgram.Builder builder = PeerPressureVertexProgram.build()
                .property(this.clusterProperty)
                .maxIterations(this.times)
                .edges(detachedTraversal);
        if (this.previousTraversalVertexProgram())
            builder.initialVoteStrength(new HaltedTraversersCountTraversal());
        return builder.create(graphs);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return TraversalParent.super.getSelfAndChildRequirements();
    }

    @Override
    public PeerPressureVertexProgramStep clone() {
        final PeerPressureVertexProgramStep clone = (PeerPressureVertexProgramStep) super.clone();
        clone.edgeTraversal = this.edgeTraversal.clone();
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        this.integrateChild(this.edgeTraversal.get());
    }
}
