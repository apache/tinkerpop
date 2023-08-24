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
package org.apache.tinkerpop.gremlin.language.grammar;

import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.PageRank;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.PeerPressure;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ShortestPath;
import org.apache.tinkerpop.gremlin.process.traversal.IO;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.WithOptions;

/**
 * Covers {@code String} oriented constants used as arguments to {@link GraphTraversal#with(String)} steps.
 */
public class WithOptionsVisitor extends DefaultGremlinBaseVisitor<Object> {

    private WithOptionsVisitor() {}

    private static WithOptionsVisitor instance;

    public static WithOptionsVisitor instance() {
        if (instance == null) {
            instance = new WithOptionsVisitor();
        }
        return instance;
    }

    @Override
    public Object visitConnectedComponentConstants(GremlinParser.ConnectedComponentConstantsContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitWithOptionKeys(GremlinParser.WithOptionKeysContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitPageRankConstants(GremlinParser.PageRankConstantsContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitPeerPressureConstants(GremlinParser.PeerPressureConstantsContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitShortestPathConstants(GremlinParser.ShortestPathConstantsContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitWithOptionsValues(GremlinParser.WithOptionsValuesContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitIoOptionsKeys(GremlinParser.IoOptionsKeysContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitIoOptionsValues(GremlinParser.IoOptionsValuesContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitConnectedComponentConstants_component(GremlinParser.ConnectedComponentConstants_componentContext ctx) {
        return super.visitConnectedComponentConstants_component(ctx);
    }

    @Override
    public Object visitConnectedComponentConstants_edges(GremlinParser.ConnectedComponentConstants_edgesContext ctx) {
        return super.visitConnectedComponentConstants_edges(ctx);
    }

    @Override
    public Object visitConnectedComponentConstants_propertyName(GremlinParser.ConnectedComponentConstants_propertyNameContext ctx) {
        return super.visitConnectedComponentConstants_propertyName(ctx);
    }

    @Override
    public Object visitPageRankConstants_edges(final GremlinParser.PageRankConstants_edgesContext ctx) {
        return PageRank.edges;
    }

    @Override
    public Object visitPageRankConstants_times(final GremlinParser.PageRankConstants_timesContext ctx) {
        return PageRank.times;
    }

    @Override
    public Object visitPageRankConstants_propertyName(final GremlinParser.PageRankConstants_propertyNameContext ctx) {
        return PageRank.propertyName;
    }

    @Override
    public Object visitPeerPressureConstants_edges(final GremlinParser.PeerPressureConstants_edgesContext ctx) {
        return PeerPressure.edges;
    }

    @Override
    public Object visitPeerPressureConstants_times(final GremlinParser.PeerPressureConstants_timesContext ctx) {
        return PeerPressure.times;
    }

    @Override
    public Object visitPeerPressureConstants_propertyName(final GremlinParser.PeerPressureConstants_propertyNameContext ctx) {
        return PeerPressure.propertyName;
    }

    @Override
    public Object visitShortestPathConstants_target(final GremlinParser.ShortestPathConstants_targetContext ctx) {
        return ShortestPath.target;
    }

    @Override
    public Object visitShortestPathConstants_edges(final GremlinParser.ShortestPathConstants_edgesContext ctx) {
        return ShortestPath.edges;
    }

    @Override
    public Object visitShortestPathConstants_distance(final GremlinParser.ShortestPathConstants_distanceContext ctx) {
        return ShortestPath.distance;
    }

    @Override
    public Object visitShortestPathConstants_maxDistance(final GremlinParser.ShortestPathConstants_maxDistanceContext ctx) {
        return ShortestPath.maxDistance;
    }

    @Override
    public Object visitShortestPathConstants_includeEdges(final GremlinParser.ShortestPathConstants_includeEdgesContext ctx) {
        return ShortestPath.includeEdges;
    }

    @Override
    public Object visitWithOptionsConstants_tokens(final GremlinParser.WithOptionsConstants_tokensContext ctx) {
        return WithOptions.tokens;
    }

    @Override
    public Object visitWithOptionsConstants_none(final GremlinParser.WithOptionsConstants_noneContext ctx) {
        return WithOptions.none;
    }

    @Override
    public Object visitWithOptionsConstants_ids(final GremlinParser.WithOptionsConstants_idsContext ctx) {
        return WithOptions.ids;
    }

    @Override
    public Object visitWithOptionsConstants_labels(final GremlinParser.WithOptionsConstants_labelsContext ctx) {
        return WithOptions.labels;
    }

    @Override
    public Object visitWithOptionsConstants_keys(final GremlinParser.WithOptionsConstants_keysContext ctx) {
        return WithOptions.keys;
    }

    @Override
    public Object visitWithOptionsConstants_values(final GremlinParser.WithOptionsConstants_valuesContext ctx) {
        return WithOptions.values;
    }

    @Override
    public Object visitWithOptionsConstants_all(final GremlinParser.WithOptionsConstants_allContext ctx) {
        return WithOptions.all;
    }

    @Override
    public Object visitWithOptionsConstants_indexer(final GremlinParser.WithOptionsConstants_indexerContext ctx) {
        return WithOptions.indexer;
    }

    @Override
    public Object visitWithOptionsConstants_list(final GremlinParser.WithOptionsConstants_listContext ctx) {
        return WithOptions.list;
    }

    @Override
    public Object visitWithOptionsConstants_map(final GremlinParser.WithOptionsConstants_mapContext ctx) {
        return WithOptions.map;
    }

    @Override
    public Object visitIoOptionsConstants_reader(final GremlinParser.IoOptionsConstants_readerContext ctx) {
        return IO.reader;
    }

    @Override
    public Object visitIoOptionsConstants_writer(final GremlinParser.IoOptionsConstants_writerContext ctx) {
        return IO.writer;
    }

    @Override
    public Object visitIoOptionsConstants_gryo(final GremlinParser.IoOptionsConstants_gryoContext ctx) {
        return IO.gryo;
    }

    @Override
    public Object visitIoOptionsConstants_graphson(final GremlinParser.IoOptionsConstants_graphsonContext ctx) {
        return IO.graphson;
    }

    @Override
    public Object visitIoOptionsConstants_graphml(final GremlinParser.IoOptionsConstants_graphmlContext ctx) {
        return IO.graphml;
    }
}
