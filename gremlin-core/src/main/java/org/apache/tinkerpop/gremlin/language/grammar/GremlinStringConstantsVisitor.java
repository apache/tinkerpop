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

import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ConnectedComponent;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.PageRank;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.PeerPressure;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ShortestPath;
import org.apache.tinkerpop.gremlin.process.traversal.IO;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.WithOptions;

/**
 * Covers {@code String} oriented constants used as arguments to {@link GraphTraversal#with(String)} steps.
 */
public class GremlinStringConstantsVisitor extends GremlinBaseVisitor<Object> {

    private GremlinStringConstantsVisitor() {}

    private static GremlinStringConstantsVisitor instance;

    public static GremlinStringConstantsVisitor instance() {
        if (instance == null) {
            instance = new GremlinStringConstantsVisitor();
        }
        return instance;
    }

    /**
     * @deprecated As of release 3.5.2, replaced by {@link #instance()}
     */
    public static GremlinStringConstantsVisitor getInstance() {
        return instance();
    }

    @Override
    public Object visitGremlinStringConstants(final GremlinParser.GremlinStringConstantsContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitConnectedComponentStringConstant(final GremlinParser.ConnectedComponentStringConstantContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitPageRankStringConstants(final GremlinParser.PageRankStringConstantsContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitPeerPressureStringConstants(final GremlinParser.PeerPressureStringConstantsContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitShortestPathStringConstants(final GremlinParser.ShortestPathStringConstantsContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitWithOptionsStringConstants(final GremlinParser.WithOptionsStringConstantsContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitIoOptionsStringConstants(final GremlinParser.IoOptionsStringConstantsContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitGremlinStringConstants_connectedComponentStringConstants_edges(final GremlinParser.GremlinStringConstants_connectedComponentStringConstants_edgesContext ctx) {
        return ConnectedComponent.edges;
    }

    @Override
    public Object visitGremlinStringConstants_connectedComponentStringConstants_component(final GremlinParser.GremlinStringConstants_connectedComponentStringConstants_componentContext ctx) {
        return ConnectedComponent.component;
    }

    @Override
    public Object visitGremlinStringConstants_connectedComponentStringConstants_propertyName(final GremlinParser.GremlinStringConstants_connectedComponentStringConstants_propertyNameContext ctx) {
        return ConnectedComponent.propertyName;
    }

    @Override
    public Object visitGremlinStringConstants_pageRankStringConstants_edges(final GremlinParser.GremlinStringConstants_pageRankStringConstants_edgesContext ctx) {
        return PageRank.edges;
    }

    @Override
    public Object visitGremlinStringConstants_pageRankStringConstants_times(final GremlinParser.GremlinStringConstants_pageRankStringConstants_timesContext ctx) {
        return PageRank.times;
    }

    @Override
    public Object visitGremlinStringConstants_pageRankStringConstants_propertyName(final GremlinParser.GremlinStringConstants_pageRankStringConstants_propertyNameContext ctx) {
        return PageRank.propertyName;
    }

    @Override
    public Object visitGremlinStringConstants_peerPressureStringConstants_edges(final GremlinParser.GremlinStringConstants_peerPressureStringConstants_edgesContext ctx) {
        return PeerPressure.edges;
    }

    @Override
    public Object visitGremlinStringConstants_peerPressureStringConstants_times(final GremlinParser.GremlinStringConstants_peerPressureStringConstants_timesContext ctx) {
        return PeerPressure.times;
    }

    @Override
    public Object visitGremlinStringConstants_peerPressureStringConstants_propertyName(final GremlinParser.GremlinStringConstants_peerPressureStringConstants_propertyNameContext ctx) {
        return PeerPressure.propertyName;
    }

    @Override
    public Object visitGremlinStringConstants_shortestPathStringConstants_target(final GremlinParser.GremlinStringConstants_shortestPathStringConstants_targetContext ctx) {
        return ShortestPath.target;
    }

    @Override
    public Object visitGremlinStringConstants_shortestPathStringConstants_edges(final GremlinParser.GremlinStringConstants_shortestPathStringConstants_edgesContext ctx) {
        return ShortestPath.edges;
    }

    @Override
    public Object visitGremlinStringConstants_shortestPathStringConstants_distance(final GremlinParser.GremlinStringConstants_shortestPathStringConstants_distanceContext ctx) {
        return ShortestPath.distance;
    }

    @Override
    public Object visitGremlinStringConstants_shortestPathStringConstants_maxDistance(final GremlinParser.GremlinStringConstants_shortestPathStringConstants_maxDistanceContext ctx) {
        return ShortestPath.maxDistance;
    }

    @Override
    public Object visitGremlinStringConstants_shortestPathStringConstants_includeEdges(final GremlinParser.GremlinStringConstants_shortestPathStringConstants_includeEdgesContext ctx) {
        return ShortestPath.includeEdges;
    }

    @Override
    public Object visitGremlinStringConstants_withOptionsStringConstants_tokens(final GremlinParser.GremlinStringConstants_withOptionsStringConstants_tokensContext ctx) {
        return WithOptions.tokens;
    }

    @Override
    public Object visitGremlinStringConstants_withOptionsStringConstants_none(final GremlinParser.GremlinStringConstants_withOptionsStringConstants_noneContext ctx) {
        return WithOptions.none;
    }

    @Override
    public Object visitGremlinStringConstants_withOptionsStringConstants_ids(final GremlinParser.GremlinStringConstants_withOptionsStringConstants_idsContext ctx) {
        return WithOptions.ids;
    }

    @Override
    public Object visitGremlinStringConstants_withOptionsStringConstants_labels(final GremlinParser.GremlinStringConstants_withOptionsStringConstants_labelsContext ctx) {
        return WithOptions.labels;
    }

    @Override
    public Object visitGremlinStringConstants_withOptionsStringConstants_keys(final GremlinParser.GremlinStringConstants_withOptionsStringConstants_keysContext ctx) {
        return WithOptions.keys;
    }

    @Override
    public Object visitGremlinStringConstants_withOptionsStringConstants_values(final GremlinParser.GremlinStringConstants_withOptionsStringConstants_valuesContext ctx) {
        return WithOptions.values;
    }

    @Override
    public Object visitGremlinStringConstants_withOptionsStringConstants_all(final GremlinParser.GremlinStringConstants_withOptionsStringConstants_allContext ctx) {
        return WithOptions.all;
    }

    @Override
    public Object visitGremlinStringConstants_withOptionsStringConstants_indexer(final GremlinParser.GremlinStringConstants_withOptionsStringConstants_indexerContext ctx) {
        return WithOptions.indexer;
    }

    @Override
    public Object visitGremlinStringConstants_withOptionsStringConstants_list(final GremlinParser.GremlinStringConstants_withOptionsStringConstants_listContext ctx) {
        return WithOptions.list;
    }

    @Override
    public Object visitGremlinStringConstants_withOptionsStringConstants_map(final GremlinParser.GremlinStringConstants_withOptionsStringConstants_mapContext ctx) {
        return WithOptions.map;
    }

    @Override
    public Object visitGremlinStringConstants_ioOptionsStringConstants_reader(final GremlinParser.GremlinStringConstants_ioOptionsStringConstants_readerContext ctx) {
        return IO.reader;
    }

    @Override
    public Object visitGremlinStringConstants_ioOptionsStringConstants_writer(final GremlinParser.GremlinStringConstants_ioOptionsStringConstants_writerContext ctx) {
        return IO.writer;
    }

    @Override
    public Object visitGremlinStringConstants_ioOptionsStringConstants_gryo(final GremlinParser.GremlinStringConstants_ioOptionsStringConstants_gryoContext ctx) {
        return IO.gryo;
    }

    @Override
    public Object visitGremlinStringConstants_ioOptionsStringConstants_graphson(final GremlinParser.GremlinStringConstants_ioOptionsStringConstants_graphsonContext ctx) {
        return IO.graphson;
    }

    @Override
    public Object visitGremlinStringConstants_ioOptionsStringConstants_graphml(final GremlinParser.GremlinStringConstants_ioOptionsStringConstants_graphmlContext ctx) {
        return IO.graphml;
    }
}
