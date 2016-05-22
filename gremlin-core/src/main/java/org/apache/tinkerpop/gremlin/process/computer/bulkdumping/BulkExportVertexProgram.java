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
package org.apache.tinkerpop.gremlin.process.computer.bulkdumping;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.process.computer.VertexComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.AbstractVertexProgramBuilder;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.javatuples.Tuple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class BulkExportVertexProgram implements VertexProgram<Tuple> {

    public static final String BULK_EXPORT_VERTEX_PROGRAM_CFG_PREFIX = "gremlin.bulkExportVertexProgram";
    public static final String BULK_EXPORT_PROPERTIES = String.join(".", BULK_EXPORT_VERTEX_PROGRAM_CFG_PREFIX, "properties");

    private Configuration configuration;
    private String properties;
    private Set<VertexComputeKey> vertexComputeKeys;

    private BulkExportVertexProgram() {
    }

    @Override
    public void loadState(final Graph graph, final Configuration config) {
        configuration = new BaseConfiguration();
        if (config != null) {
            ConfigurationUtils.copy(config, configuration);
        }
        properties = configuration.getString(BULK_EXPORT_PROPERTIES);
        vertexComputeKeys = Collections.singleton(VertexComputeKey.of(BULK_EXPORT_PROPERTIES, false));
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
    }

    @Override
    public void execute(final Vertex sourceVertex, final Messenger<Tuple> messenger, final Memory memory) {
        final VertexProperty<TraverserSet> haltedTraversers = sourceVertex.property(TraversalVertexProgram.HALTED_TRAVERSERS);
        if (haltedTraversers.isPresent()) {
            sourceVertex.property(BULK_EXPORT_PROPERTIES, properties);
        }
    }

    @Override
    public boolean terminate(final Memory memory) {
        return properties == null || properties.isEmpty() || !memory.isInitialIteration();
    }

    @Override
    public Set<MessageScope> getMessageScopes(final Memory memory) {
        return Collections.emptySet();
    }

    @SuppressWarnings({"CloneDoesntDeclareCloneNotSupportedException", "CloneDoesntCallSuperClone"})
    @Override
    public VertexProgram<Tuple> clone() {
        return this;
    }

    @Override
    public GraphComputer.ResultGraph getPreferredResultGraph() {
        return GraphComputer.ResultGraph.NEW;
    }

    @Override
    public GraphComputer.Persist getPreferredPersist() {
        return GraphComputer.Persist.EDGES;
    }

    @Override
    public Set<VertexComputeKey> getVertexComputeKeys() {
        return this.vertexComputeKeys;
    }

    @Override
    public String toString() {
        return StringFactory.vertexProgramString(this);
    }

    public static Builder build() {
        return new Builder();
    }

    public static class Builder extends AbstractVertexProgramBuilder<Builder> {

        private List<String> properties;

        private Builder() {
            super(BulkExportVertexProgram.class);
            properties = new ArrayList<>();
        }

        @SuppressWarnings("unchecked")
        @Override
        public BulkExportVertexProgram create(final Graph graph) {
            configuration.setProperty(BULK_EXPORT_PROPERTIES, String.join("\1", properties));
            graph.configuration().setProperty("gremlin.hadoop.graphWriter", "org.apache.tinkerpop.gremlin.hadoop.structure.io.script.ScriptOutputFormat");
            return (BulkExportVertexProgram) VertexProgram.createVertexProgram(graph, configuration);
        }

        public Builder key(final String key, final String format) {
            properties.add(key + "\2" + format);
            return this;
        }

        public Builder key(final String key) {
            this.key(key, "");
            return this;
        }

        public Builder keys(final String... keys) {
            for (final String key : keys) {
                this.key(key, "");
            }
            return this;
        }
    }
}
