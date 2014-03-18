package com.tinkerpop.gremlin.algorithm.generator;

import java.util.Random;

/**
 * Generates values according to a normal distribution with the configured standard deviation.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class NormalDistribution implements Distribution {

    private final double stdDeviation;
    private final double mean;

    /**
     * Constructs a NormalDistribution with the given standard deviation.
     * <p/>
     * Setting the standard deviation to 0 makes this a constant distribution.
     *
     * @param stdDeviation Simple deviation of the distribution. Must be non-negative.
     */
    public NormalDistribution(final double stdDeviation) {
        this(stdDeviation, 0.0);
    }

    private NormalDistribution(final double stdDeviation, final double mean) {
        if (stdDeviation < 0)
            throw new IllegalArgumentException("Standard deviation must be non-negative: " + stdDeviation);
        if (mean < 0) throw new IllegalArgumentException("Mean must be positive: " + mean);
        this.stdDeviation = stdDeviation;
        this.mean = mean;
    }

    @Override
    public Distribution initialize(final int invocations, final int expectedTotal) {
        double mean = (expectedTotal * 1.0) / invocations; //TODO: account for truncated gaussian distribution
        return new NormalDistribution(stdDeviation, mean);
    }

    @Override
    public int nextValue(final Random random) {
        if (mean == 0.0) throw new IllegalStateException("Distribution has not been initialized");
        return (int) Math.round(random.nextGaussian() * stdDeviation + mean);
    }

    @Override
    public int nextConditionalValue(final Random random, final int otherValue) {
        return nextValue(random);
    }

    @Override
    public String toString() {
        return "NormalDistribution{" +
                "stdDeviation=" + stdDeviation +
                ", mean=" + mean +
                '}';
    }
}
