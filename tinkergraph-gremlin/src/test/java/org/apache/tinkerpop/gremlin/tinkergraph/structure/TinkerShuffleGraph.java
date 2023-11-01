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
package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * @author Cole Greer (https://github.com/Cole-Greer)
 */
public class TinkerShuffleGraph extends TinkerGraph {

    private static final Configuration EMPTY_CONFIGURATION = new BaseConfiguration() {{
        this.setProperty(Graph.GRAPH, TinkerShuffleGraph.class.getName());
    }};

    private TinkerShuffleGraph(Configuration configuration) {
        super(configuration);
    }

    public static TinkerShuffleGraph open() {
        return open(EMPTY_CONFIGURATION);
    }

    public static TinkerShuffleGraph open(Configuration configuration) {
        return new TinkerShuffleGraph(configuration);
    }

    @Override
    public Iterator<Vertex> vertices(final Object... vertexIds) {
        return shuffleIterator(super.vertices(vertexIds));
    }

    @Override
    public Iterator<Edge> edges(final Object... edgeIds) {
        return shuffleIterator(super.edges(edgeIds));
    }

    @Override
    protected TinkerVertex createTinkerVertex(final Object id, final String label, final AbstractTinkerGraph graph) {
        return new TinkerShuffleVertex(id, label, graph);
    }

    protected TinkerVertex createTinkerVertex(final Object id, final String label, final AbstractTinkerGraph graph, final long currentVersion) {
        return new TinkerShuffleVertex(id, label, graph, currentVersion);
    }

    protected TinkerEdge createTinkerEdge(final Object id, final Vertex outVertex, final String label, final Vertex inVertex) {
        return new TinkerShuffleEdge(id, outVertex, label, inVertex);
    }

    protected TinkerEdge createTinkerEdge(final Object id, final Vertex outVertex, final String label, final Vertex inVertex, final long currentVersion) {
        return new TinkerShuffleEdge(id, outVertex, label, inVertex, currentVersion);
    }

    static Iterator shuffleIterator(Iterator iter) {
        List list = IteratorUtils.toList(iter);
        Collections.shuffle(list);
        return list.iterator();
    }

}
