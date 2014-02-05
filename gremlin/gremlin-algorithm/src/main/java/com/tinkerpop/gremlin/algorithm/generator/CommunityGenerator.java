package com.tinkerpop.gremlin.algorithm.generator;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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

    /**
     * Default value used if {@link #setCrossCommunityPercentage(double)} is not called.
     */
    public static final double DEFAULT_CROSS_COMMUNITY_PERCENTAGE = 0.1;

    private Distribution communitySize = null;
    private Distribution edgeDegree = null;
    private double crossCommunityPercentage = DEFAULT_CROSS_COMMUNITY_PERCENTAGE;

    private final Random random;

    public CommunityGenerator(final String label) {
        this(label, null);
    }

    public CommunityGenerator(final String label, final Consumer<Edge> annotator) {
        this(label, annotator, null);
    }

    public CommunityGenerator(final String label, final Consumer<Edge> edgeAnnotator, final BiConsumer<Vertex,Map<String,Object>> vertexAnnotator) {
        this(label, edgeAnnotator, vertexAnnotator, null);
    }

    /**
     * @see AbstractGenerator#AbstractGenerator(String, java.util.Optional, java.util.Optional, java.util.Optional)
     */
    public CommunityGenerator(final String label, final Consumer<Edge> edgeAnnotator,
                              final BiConsumer<Vertex,Map<String,Object>> vertexAnnotator,
                              final Supplier<Long> seedGenerator) {
        super(label, Optional.ofNullable(edgeAnnotator), Optional.ofNullable(vertexAnnotator), Optional.ofNullable(seedGenerator));
        random = new Random(this.seedSupplier.get());
    }

    /**
     * Sets the distribution to be used to generate the sizes of communities.
     */
    public void setCommunityDistribution(final Distribution community) {
        this.communitySize = community;
    }

    /**
     * Sets the distribution to be used to generate the out-degrees of vertices.
     */
    public void setDegreeDistribution(final Distribution degree) {
        this.edgeDegree = degree;
    }

    /**
     * Sets the percentage of edges that cross a community, i.e. connect a vertex to a vertex in
     * another community. The lower this value, the higher the modularity of the generated communities.
     *
     * @param percentage Percentage of community crossing edges. Must be in [0,1]
     */
    public void setCrossCommunityPercentage(final double percentage) {
        if (percentage < 0.0 || percentage > 1.0)
            throw new IllegalArgumentException("Percentage must be between 0 and 1");
        this.crossCommunityPercentage = percentage;
    }

    /**
     * Returns the configured cross community percentage.
     */
    @SuppressWarnings("UnusedDeclaration")
    public double getCrossCommunityPercentage() {
        return crossCommunityPercentage;
    }

    /**
     * Generates a synthetic network for all vertices in the given graph such that the provided expected number
     * of communities are generated with the specified expected number of edges.
     *
     * @return The actual number of edges generated. May be different from the expected number.
     */
    public int generate(final Graph graph, final int expectedNumCommunities, final int expectedNumEdges) {
        return generate(graph.query().vertices(), expectedNumCommunities, expectedNumEdges);
    }


    /**
     * Generates a synthetic network for provided vertices in the given graph such that the provided expected number
     * of communities are generated with the specified expected number of edges.
     *
     * @return The actual number of edges generated. May be different from the expected number.
     */
    public int generate(final Iterable<Vertex> vertices, final int expectedNumCommunities, final int expectedNumEdges) {
        if (communitySize == null) throw new IllegalStateException("Need to initialize community size distribution");
        if (edgeDegree == null) throw new IllegalStateException("Need to initialize degree distribution");
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
                        while (selected == null && tries < 100) {
                            // choose another community to connect to and make sure it's not in the current
                            // community of the current vertex
                            while (othercomm == null) {
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

}
