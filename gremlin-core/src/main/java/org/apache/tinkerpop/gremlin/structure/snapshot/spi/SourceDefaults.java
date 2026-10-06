/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.structure.snapshot.spi;

import java.util.Objects;

/**
 * The defaults and cardinalities of the source graph, recorded in the manifest so that a graph over the snapshot can
 * report the same behavior. They do not affect how data is stored.
 */
public final class SourceDefaults {

    /**
     * The defaults of a source that does not report any: labels {@code "vertex"} and {@code "edge"}, vertex label
     * cardinality {@code "ONE"} and default vertex-property cardinality {@code "single"}.
     */
    public static final SourceDefaults DEFAULT = new SourceDefaults("vertex", "edge", "ONE", "single");

    private final String defaultVertexLabel;
    private final String defaultEdgeLabel;
    private final String vertexLabelCardinality;
    private final String defaultVertexPropertyCardinality;

    /**
     * @param defaultVertexLabel               the label given to a vertex added without one
     * @param defaultEdgeLabel                 the label given to an edge added without one
     * @param vertexLabelCardinality           the name of the vertex label cardinality, for example {@code "ONE"},
     *                                         {@code "ONE_OR_MORE"} or {@code "ZERO_OR_MORE"}
     * @param defaultVertexPropertyCardinality {@code "single"}, {@code "list"} or {@code "set"}
     */
    public SourceDefaults(final String defaultVertexLabel, final String defaultEdgeLabel,
                          final String vertexLabelCardinality, final String defaultVertexPropertyCardinality) {
        this.defaultVertexLabel = Objects.requireNonNull(defaultVertexLabel);
        this.defaultEdgeLabel = Objects.requireNonNull(defaultEdgeLabel);
        this.vertexLabelCardinality = Objects.requireNonNull(vertexLabelCardinality);
        this.defaultVertexPropertyCardinality = Objects.requireNonNull(defaultVertexPropertyCardinality);
    }

    public String defaultVertexLabel() {
        return defaultVertexLabel;
    }

    public String defaultEdgeLabel() {
        return defaultEdgeLabel;
    }

    public String vertexLabelCardinality() {
        return vertexLabelCardinality;
    }

    public String defaultVertexPropertyCardinality() {
        return defaultVertexPropertyCardinality;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof SourceDefaults)) return false;
        final SourceDefaults that = (SourceDefaults) o;
        return defaultVertexLabel.equals(that.defaultVertexLabel) && defaultEdgeLabel.equals(that.defaultEdgeLabel)
                && vertexLabelCardinality.equals(that.vertexLabelCardinality)
                && defaultVertexPropertyCardinality.equals(that.defaultVertexPropertyCardinality);
    }

    @Override
    public int hashCode() {
        return Objects.hash(defaultVertexLabel, defaultEdgeLabel, vertexLabelCardinality,
                defaultVertexPropertyCardinality);
    }

    @Override
    public String toString() {
        return "SourceDefaults{vertexLabel=" + defaultVertexLabel + ", edgeLabel=" + defaultEdgeLabel
                + ", vertexLabelCardinality=" + vertexLabelCardinality + ", vertexPropertyCardinality="
                + defaultVertexPropertyCardinality + '}';
    }
}
