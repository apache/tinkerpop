package com.tinkerpop.gremlin.algorithm.generator;

import java.util.Random;

/**
 * CopyDistribution returns the conditional value.
 * <p/>
 * Hence, this class can be used as the in-degree distribution to ensure that
 * the in-degree of a vertex is equal to the out-degree.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class CopyDistribution implements Distribution {

    @Override
    public Distribution initialize(final int invocations, final int expectedTotal) {
        return this;
    }

    @Override
    public int nextValue(final Random random) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int nextConditionalValue(final Random random, final int otherValue) {
        return otherValue;
    }
}
