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
package org.apache.tinkerpop.gremlin.process.computer.clustering.connected;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.process.computer.VertexComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.AbstractVertexProgramBuilder;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.util.PureTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Identifies "Connected Component" instances in a graph by assigning a component identifier (the lexicographically
 * least string value of the vertex in the component) to each vertex.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class ConnectedComponentVertexProgram implements VertexProgram<String> {

    public static final String COMPONENT = "gremlin.connectedComponentVertexProgram.component";
    private static final String PROPERTY = "gremlin.connectedComponentVertexProgram.property";
    private static final String EDGE_TRAVERSAL = "gremlin.pageRankVertexProgram.edgeTraversal";
    private static final String HAS_HALTED = "gremlin.connectedComponentVertexProgram.hasHalted";
    private static final String VOTE_TO_HALT = "gremlin.connectedComponentVertexProgram.voteToHalt";

    private static final Set<MemoryComputeKey> MEMORY_COMPUTE_KEYS = Collections.singleton(MemoryComputeKey.of(VOTE_TO_HALT, Operator.and, false, true));
    private MessageScope.Local<?> scope = MessageScope.Local.of(__::bothE);
    private Set<MessageScope> scopes;
    private String property = COMPONENT;
    private boolean hasHalted = false;
    private PureTraversal<Vertex, Edge> edgeTraversal = null;
    private Configuration configuration;

    private ConnectedComponentVertexProgram() {}

    @Override
    public void loadState(final Graph graph, final Configuration config) {
        configuration = new BaseConfiguration();
        if (config != null) {
            ConfigurationUtils.copy(config, configuration);
        }

        if (configuration.containsKey(EDGE_TRAVERSAL)) {
            this.edgeTraversal = PureTraversal.loadState(configuration, EDGE_TRAVERSAL, graph);
            this.scope = MessageScope.Local.of(() -> this.edgeTraversal.get().clone());
        }

        scopes = new HashSet<>(Collections.singletonList(scope));

        this.property = configuration.getString(PROPERTY, COMPONENT);
        this.hasHalted = configuration.getBoolean(HAS_HALTED, false);
    }

    @Override
    public void storeState(final Configuration config) {
        VertexProgram.super.storeState(config);
        if (configuration != null) {
            ConfigurationUtils.copy(configuration, config);
        }
    }

    @Override
    public void setup(final Memory memory) {
        memory.set(VOTE_TO_HALT, true);
    }

    @Override
    public void execute(final Vertex vertex, final Messenger<String> messenger, final Memory memory) {
        if (memory.isInitialIteration()) {
            // on the first pass, just initialize the component to its own id then pass it to all adjacent vertices
            // for evaluation
            vertex.property(VertexProperty.Cardinality.single, property, vertex.id().toString());

            // no need to send messages from traversers that were filtered - happens for stuff like
            // g.V().hasLabel('software').connectedComponent() (i.e. when there is a TraversalVertexProgram in the mix
            // halting traversers. only want to send messages from traversers that are still hanging about after
            // the filter. the unfiltered vertices can only react to messages sent to them. of course, this can
            // lead to weirdness in results. 
            if (vertex.edges(Direction.BOTH).hasNext() && !(hasHalted && !vertex.property(TraversalVertexProgram.HALTED_TRAVERSERS).isPresent())) {
                // since there was message passing we don't want to halt on the first round. this should only trigger
                // a single pass finish if the graph is completely disconnected (technically, it won't even really
                // work in cases where halted traversers come into play
                messenger.sendMessage(scope, vertex.id().toString());
                memory.add(VOTE_TO_HALT, false);
            }
        } else {
            // by the second iteration all vertices that matter should have a component assigned
            String currentComponent = vertex.value(property);
            boolean different = false;

            // iterate through messages received and determine if there is a component that has a lesser value than
            // the currently assigned one
            final Iterator<String> componentIterator = messenger.receiveMessages();
            while(componentIterator.hasNext()) {
                final String candidateComponent = componentIterator.next();
                if (candidateComponent.compareTo(currentComponent) < 0) {
                    currentComponent = candidateComponent;
                    different = true;
                }
            }

            // if there is a better component then assign it and notify adjacent vertices. triggering the message
            // passing should not halt future executions
            if (different) {
                vertex.property(VertexProperty.Cardinality.single, property, currentComponent);
                messenger.sendMessage(scope, currentComponent);
                memory.add(VOTE_TO_HALT, false);
            }
        }
    }

    @Override
    public Set<VertexComputeKey> getVertexComputeKeys() {
        return new HashSet<>(Collections.singletonList(VertexComputeKey.of(property, false)));
    }

    @Override
    public Set<MemoryComputeKey> getMemoryComputeKeys() {
        return MEMORY_COMPUTE_KEYS;
    }

    @Override
    public boolean terminate(final Memory memory) {
        final boolean voteToHalt = memory.<Boolean>get(VOTE_TO_HALT);
        if (voteToHalt) {
            return true;
        } else {
            // it is basically always assumed that the program will want to halt, but if message passing occurs, the
            // program will want to continue, thus reset false values to true for future iterations
            memory.set(VOTE_TO_HALT, true);
            return false;
        }
    }

    @Override
    public Set<MessageScope> getMessageScopes(final Memory memory) {
        return scopes;
    }

    @Override
    public GraphComputer.ResultGraph getPreferredResultGraph() {
        return GraphComputer.ResultGraph.NEW;
    }

    @Override
    public GraphComputer.Persist getPreferredPersist() {
        return GraphComputer.Persist.VERTEX_PROPERTIES;
    }


    @Override
    @SuppressWarnings("CloneDoesntCallSuperClone,CloneDoesntDeclareCloneNotSupportedException")
    public ConnectedComponentVertexProgram clone() {
        return this;
    }

    @Override
    public Features getFeatures() {
        return new Features() {
            @Override
            public boolean requiresLocalMessageScopes() {
                return true;
            }

            @Override
            public boolean requiresVertexPropertyAddition() {
                return true;
            }
        };
    }

    public static ConnectedComponentVertexProgram.Builder build() {
        return new ConnectedComponentVertexProgram.Builder();
    }

    public static final class Builder extends AbstractVertexProgramBuilder<ConnectedComponentVertexProgram.Builder> {

        private Builder() {
            super(ConnectedComponentVertexProgram.class);
        }

        public ConnectedComponentVertexProgram.Builder hasHalted(final boolean hasHalted) {
            this.configuration.setProperty(HAS_HALTED, hasHalted);
            return this;
        }

        public ConnectedComponentVertexProgram.Builder edges(final Traversal.Admin<Vertex, Edge> edgeTraversal) {
            PureTraversal.storeState(this.configuration, EDGE_TRAVERSAL, edgeTraversal);
            return this;
        }

        public ConnectedComponentVertexProgram.Builder property(final String key) {
            this.configuration.setProperty(PROPERTY, key);
            return this;
        }
    }
}
