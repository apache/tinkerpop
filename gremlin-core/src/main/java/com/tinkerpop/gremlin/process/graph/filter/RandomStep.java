package com.tinkerpop.gremlin.process.graph.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Random;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RandomStep<S> extends FilterStep<S> {

    private final Random random = new Random();
    public double probability;

    public RandomStep(final Traversal traversal, final double probability) {
        super(traversal);
        this.probability = probability;
        this.setPredicate(holder -> this.probability >= this.random.nextDouble());
    }

    public String toString() {
        return TraversalHelper.makeStepString(this, this.probability);
    }
}
