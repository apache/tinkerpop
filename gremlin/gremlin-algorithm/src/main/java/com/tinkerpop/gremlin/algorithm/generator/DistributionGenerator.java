package com.tinkerpop.gremlin.algorithm.generator;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;

/**
 * Generates a synthetic network for a given out- and (optionally) in-degree distribution.
 * <p/>
 * After construction, at least the out-degree distribution must be set via {@link #setOutDistribution}
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DistributionGenerator extends AbstractGenerator {

    private Distribution outDistribution;
    private Distribution inDistribution;

    private boolean allowLoops = true;

    public DistributionGenerator(final String label) {
        super(label);
    }

    @SuppressWarnings("UnusedDeclaration")
    public DistributionGenerator(final String label, final Consumer<Edge> annotator) {
        this(label, annotator, null);
    }

    /**
     * @see AbstractGenerator#AbstractGenerator(String, java.util.Optional, java.util.Optional, java.util.Optional)
     */
    public DistributionGenerator(final String label, final Consumer<Edge> annotator, final Supplier<Long> seedGenerator) {
        super(label, Optional.ofNullable(annotator), Optional.empty(), Optional.ofNullable(seedGenerator));
    }

    /**
     * Sets the out-degree distribution to be used by this generator.
     * <p/>
     * This method must be called prior to generating the network.
     */
    public void setOutDistribution(final Distribution distribution) {
        if (distribution == null) throw new NullPointerException();
        this.outDistribution = distribution;
    }

    /**
     * Sets the in-degree distribution to be used by this generator.
     * <p/>
     * If the in-degree distribution is not specified, {@link CopyDistribution} is used by default.
     */
    @SuppressWarnings("UnusedDeclaration")
    public void setInDistribution(final Distribution distribution) {
        if (distribution == null) throw new NullPointerException();
        this.inDistribution = distribution;
    }

    /**
     * Clears the in-degree distribution
     */
    @SuppressWarnings("UnusedDeclaration")
    public void clearInDistribution() {
        this.inDistribution = null;
    }

    /**
     * Whether edge loops are allowed
     */
    @SuppressWarnings("UnusedDeclaration")
    public boolean hasAllowLoops() {
        return allowLoops;
    }

    /**
     * Sets whether loops, i.e. edges with the same start and end vertex, are allowed to be generated.
     */
    @SuppressWarnings("UnusedDeclaration")
    public void setAllowLoops(final boolean allowLoops) {
        this.allowLoops = allowLoops;
    }

    /**
     * Generates a synthetic network connecting all vertices in the provided graph with the expected number
     * of edges.
     *
     * @return The number of generated edges. Not that this number may not be equal to the expected number of edges
     */
    public int generate(final Graph graph, final int expectedNumEdges) {
        return generate(graph.V().toList(), expectedNumEdges);
    }

    /**
     * Generates a synthetic network connecting the given vertices by the expected number of directed edges
     * in the provided graph.
     *
     * @return The number of generated edges. Not that this number may not be equal to the expected number of edges
     */
    public int generate(final Iterable<Vertex> vertices, final int expectedNumEdges) {
        return generate(vertices, vertices, expectedNumEdges);
    }

    /**
     * Generates a synthetic network connecting the vertices in <i>out</i> by directed edges
     * with those in <i>in</i> with the given number of expected edges in the provided graph.
     *
     * @return The number of generated edges. Not that this number may not be equal to the expected number of edges
     */
    public int generate(final Iterable<Vertex> out, final Iterable<Vertex> in, final int expectedNumEdges) {
        // todo: consider transactions...batchgraph??
        if (outDistribution == null)
            throw new IllegalStateException("Must set out-distribution before generating edges");

        final Distribution outDist = outDistribution.initialize(SizableIterable.sizeOf(out), expectedNumEdges);
        Distribution inDist;
        if (inDistribution == null) {
            if (out != in) throw new IllegalArgumentException("Need to specify in-distribution");
            inDist = new CopyDistribution();
        } else {
            inDist = inDistribution.initialize(SizableIterable.sizeOf(in), expectedNumEdges);
        }

        final long seed = this.seedSupplier.get();
        Random outRandom = new Random(seed);
        final ArrayList<Vertex> outStubs = new ArrayList<>(expectedNumEdges);
        for (Vertex v : out) {
            final int degree = outDist.nextValue(outRandom);
            IntStream.range(0, degree).forEach(i->outStubs.add(v));
        }

        Collections.shuffle(outStubs);

        outRandom = new Random(seed);
        final Random inRandom = new Random(this.seedSupplier.get());
        int addedEdges = 0;
        int position = 0;
        for (Vertex v : in) {
            final int degree = inDist.nextConditionalValue(inRandom, outDist.nextValue(outRandom));
            for (int i = 0; i < degree; i++) {
                Vertex other = null;
                while (other == null) {
                    if (position >= outStubs.size()) return addedEdges; //No more edges to connect
                    other = outStubs.get(position);
                    position++;
                    if (!allowLoops && v.equals(other)) other = null;
                }
                //Connect edge
                addEdge(other, v);
                addedEdges++;
            }
        }
        return addedEdges;
    }


}
