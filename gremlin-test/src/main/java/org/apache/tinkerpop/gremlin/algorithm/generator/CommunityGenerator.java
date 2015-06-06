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
package org.apache.tinkerpop.gremlin.algorithm.generator;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Generates a synthetic network with a community structure, that is, several densely connected
 * sub-networks that are loosely connected with one another.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class CommunityGenerator extends AbstractGenerator {

    public static final double DEFAULT_CROSS_COMMUNITY_PERCENTAGE = 0.1;
    public static final int DEFAULT_NUMBER_OF_COMMUNITIES = 2;

    private final Distribution communitySize;
    private final Distribution edgeDegree;
    private final double crossCommunityPercentage;
    private final Iterable<Vertex> vertices;
    private final int expectedNumCommunities;
    private final int expectedNumEdges;

    private final Random random;

    private CommunityGenerator(final Graph g, final String label, final Optional<Consumer<Edge>> edgeProcessor,
                               final Optional<BiConsumer<Vertex, Map<String, Object>>> vertexProcessor,
                               final Supplier<Long> seedGenerator, final Distribution communitySize,
                               final Distribution edgeDegree, final double crossCommunityPercentage,
                               final Iterable<Vertex> vertices, final int expectedNumCommunities,
                               final int expectedNumEdges) {
        super(g, label, edgeProcessor, vertexProcessor, seedGenerator);
        random = new Random(this.seedSupplier.get());
        this.communitySize = communitySize;
        this.edgeDegree = edgeDegree;
        this.crossCommunityPercentage = crossCommunityPercentage;
        this.vertices = vertices;
        this.expectedNumCommunities = expectedNumCommunities;
        this.expectedNumEdges = expectedNumEdges;
    }

    /**
     * Generates a synthetic network for provided vertices in the given graph such that the provided expected number
     * of communities are generated with the specified expected number of edges.
     *
     * @return The actual number of edges generated. May be different from the expected number.
     */
    @Override
    public int generate() {
        int numVertices = SizableIterable.sizeOf(vertices);
        final Iterator<Vertex> iter = vertices.iterator();
        final ArrayList<ArrayList<Vertex>> communities = new ArrayList<>(expectedNumCommunities);
        final Distribution communityDist = communitySize.initialize(expectedNumCommunities, numVertices);
        final Map<String, Object> context = new HashMap<>();
        while (iter.hasNext()) {
            final int nextSize = communityDist.nextValue(random);
            context.put("communityIndex", communities.size());
            final ArrayList<Vertex> community = new ArrayList<>(nextSize);
            for (int i = 0; i < nextSize && iter.hasNext(); i++) {
                community.add(processVertex(iter.next(), context));
            }
            if (!community.isEmpty()) communities.add(community);
        }

        final double inCommunityPercentage = 1.0 - crossCommunityPercentage;
        final Distribution degreeDist = edgeDegree.initialize(numVertices, expectedNumEdges);
        if (crossCommunityPercentage > 0 && communities.size() < 2)
            throw new IllegalArgumentException("Cannot have cross links with only one community");
        int addedEdges = 0;

        //System.out.println("Generating links on communities: "+communities.size());

        for (ArrayList<Vertex> community : communities) {
            for (Vertex v : community) {
                final int randomDegree = degreeDist.nextValue(random);
                final int degree = Math.min(randomDegree, (int) Math.ceil((community.size() - 1) / inCommunityPercentage) - 1);
                final Set<Vertex> inlinks = new HashSet<>();
                final Set<Vertex> outlinks = new HashSet<>();
                for (int i = 0; i < degree; i++) {
                    Vertex selected = null;
                    if (random.nextDouble() < crossCommunityPercentage || (community.size() - 1 <= inlinks.size())) {
                        //Cross community
                        int tries = 0;
                        ArrayList<Vertex> othercomm = null;

                        // this limit on the number of tries prevents infinite loop where the selected vertex to
                        // link to doesn't exist given the nature and structure of the graph.
                        while (null == selected && tries < 100) {
                            // choose another community to connect to and make sure it's not in the current
                            // community of the current vertex
                            while (null == othercomm) {
                                othercomm = communities.get(random.nextInt(communities.size()));
                                if (othercomm.equals(community)) othercomm = null;
                            }
                            selected = othercomm.get(random.nextInt(othercomm.size()));
                            if (outlinks.contains(selected)) selected = null;

                            tries++;
                        }

                        // if tries expires then the value of selected is null in which case it should not be added.
                        if (selected != null) outlinks.add(selected);
                    } else {
                        //In community
                        int tries = 0;
                        while (selected == null && tries < 100) {
                            selected = community.get(random.nextInt(community.size()));
                            if (v.equals(selected) || inlinks.contains(selected)) selected = null;
                            tries++;
                        }

                        if (selected != null) inlinks.add(selected);
                    }

                    // only add an edge if the vertex was actually selected.
                    if (selected != null) {
                        addEdge(v, selected);
                        addedEdges++;
                    }
                }
            }
        }
        return addedEdges;
    }

    public static Builder build(final Graph g) {
        return new Builder(g);
    }

    public final static class Builder extends AbstractGeneratorBuilder<Builder> {
        private final Graph g;
        private Distribution communitySize = null;
        private Distribution edgeDegree = null;
        private double crossCommunityPercentage = DEFAULT_CROSS_COMMUNITY_PERCENTAGE;
        private Iterable<Vertex> vertices;
        private int expectedNumCommunities = DEFAULT_NUMBER_OF_COMMUNITIES;
        private int expectedNumEdges;

        private Builder(final Graph g) {
            super(Builder.class);
            this.g = g;
            final List<Vertex> allVertices = IteratorUtils.list(g.vertices());
            this.vertices = allVertices;
            this.expectedNumEdges = allVertices.size() * 2;
        }

        public Builder verticesToGenerateEdgesFor(final Iterable<Vertex> vertices) {
            this.vertices = vertices;
            return this;
        }

        public Builder expectedNumCommunities(final int expectedNumCommunities) {
            this.expectedNumCommunities = expectedNumCommunities;
            return this;
        }

        public Builder expectedNumEdges(final int expectedNumEdges) {
            this.expectedNumEdges = expectedNumEdges;
            return this;
        }

        /**
         * Sets the distribution to be used to generate the sizes of communities.
         */
        public Builder communityDistribution(final Distribution community) {
            this.communitySize = community;
            return this;
        }

        /**
         * Sets the distribution to be used to generate the out-degrees of vertices.
         */
        public Builder degreeDistribution(final Distribution degree) {
            this.edgeDegree = degree;
            return this;
        }

        /**
         * Sets the percentage of edges that cross a community, i.e. connect a vertex to a vertex in
         * another community. The lower this value, the higher the modularity of the generated communities.
         *
         * @param percentage Percentage of community crossing edges. Must be in [0,1]
         */
        public Builder crossCommunityPercentage(final double percentage) {
            if (percentage < 0.0 || percentage > 1.0)
                throw new IllegalArgumentException("Percentage must be between 0 and 1");
            this.crossCommunityPercentage = percentage;
            return this;
        }

        public CommunityGenerator create() {
            if (null == communitySize)
                throw new IllegalStateException("Need to initialize community size distribution");
            if (null == edgeDegree) throw new IllegalStateException("Need to initialize degree distribution");
            return new CommunityGenerator(this.g, this.label, this.edgeProcessor, this.vertexProcessor, this.seedSupplier,
                    this.communitySize, this.edgeDegree, crossCommunityPercentage, vertices,
                    expectedNumCommunities, expectedNumEdges);
        }
    }
}
