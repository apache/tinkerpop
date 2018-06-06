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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.Configuring;
import org.apache.tinkerpop.gremlin.process.traversal.step.TimesModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
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
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class PeerPressureVertexProgramStep extends VertexProgramStep
        implements TraversalParent, ByModulating, TimesModulating, Configuring {

    private Parameters parameters = new Parameters();
    private PureTraversal<Vertex, Edge> edgeTraversal;
    private String clusterProperty = PeerPressureVertexProgram.CLUSTER;
    private int times = 30;

    public PeerPressureVertexProgramStep(final Traversal.Admin traversal) {
        super(traversal);
        this.configure(PeerPressure.EDGES, __.<Vertex>outE().asAdmin());
    }

    @Override
    public void configure(final Object... keyValues) {
        if (keyValues[0].equals(PeerPressureVertexProgramStep.PeerPressure.EDGES)) {
            if (!(keyValues[1] instanceof Traversal))
                throw new IllegalArgumentException("PeerPressure.EDGES requires a Traversal as its argument");
            this.edgeTraversal = new PureTraversal<>(((Traversal<Vertex,Edge>) keyValues[1]).asAdmin());
            this.integrateChild(this.edgeTraversal.get());
        } else if (keyValues[0].equals(PeerPressureVertexProgramStep.PeerPressure.PROPERTY_NAME)) {
            if (!(keyValues[1] instanceof String))
                throw new IllegalArgumentException("PeerPressure.PROPERTY_NAME requires a String as its argument");
            this.clusterProperty = (String) keyValues[1];
        } else if (keyValues[0].equals(PeerPressureVertexProgramStep.PeerPressure.TIMES)) {
            if (!(keyValues[1] instanceof Integer))
                throw new IllegalArgumentException("PeerPressure.TIMES requires an Integer as its argument");
            this.times = (int) keyValues[1];
        }else {
            this.parameters.set(this, keyValues);
        }
    }

    @Override
    public Parameters getParameters() {
        return parameters;
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.edgeTraversal.hashCode() ^ this.clusterProperty.hashCode() ^ this.times;
    }

    /**
     * @deprecated As of release 3.4.0, replaced by {@link #configure(Object...)}
     */
    @Deprecated
    @Override
    public void modulateBy(final Traversal.Admin<?, ?> edgeTraversal) {
        configure(PeerPressure.EDGES, edgeTraversal);
    }

    /**
     * @deprecated As of release 3.4.0, replaced by {@link #configure(Object...)}
     */
    @Deprecated
    @Override
    public void modulateBy(final String clusterProperty) {
        configure(PeerPressure.PROPERTY_NAME, clusterProperty);
    }

    /**
     * @deprecated As of release 3.4.0, replaced by {@link #configure(Object...)}
     */
    @Deprecated
    @Override
    public void modulateTimes(int times) {
        configure(PeerPressure.TIMES, times);
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
    public PeerPressureVertexProgram generateProgram(final Graph graph, final Memory memory) {
        final Traversal.Admin<Vertex, Edge> detachedTraversal = this.edgeTraversal.getPure();
        detachedTraversal.setStrategies(TraversalStrategies.GlobalCache.getStrategies(graph.getClass()));
        final PeerPressureVertexProgram.Builder builder = PeerPressureVertexProgram.build()
                .property(this.clusterProperty)
                .maxIterations(this.times)
                .edges(detachedTraversal);
        if (this.previousTraversalVertexProgram())
            builder.initialVoteStrength(new HaltedTraversersCountTraversal());
        return builder.create(graph);
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

    /**
     * Configuration options to be passed to the {@link GraphTraversal#with(String, Object)} step.
     */
    public static class PeerPressure {
        /**
         * Configures number of iterations that the algorithm should run.
         */
        public static final String TIMES = Graph.Hidden.hide("tinkerpop.peerPressure.times");

        /**
         * Configures the edge to traverse when determining clusters.
         */
        public static final String EDGES = Graph.Hidden.hide("tinkerpop.peerPressure.edges");

        /**
         * Configures the name of the property within which to store the cluster value.
         */
        public static final String PROPERTY_NAME = Graph.Hidden.hide("tinkerpop.peerPressure.propertyName");
    }
}
