package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Collections;
import java.util.Random;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class CoinStep<S> extends FilterStep<S> implements Reversible {

    private static final Random RANDOM = new Random();
    private final double probability;

    public CoinStep(final Traversal traversal, final double probability) {
        super(traversal);
        this.probability = probability;
        this.setPredicate(traverser -> {
            long newBulk = 0l;
            for (int i = 0; i < traverser.bulk(); i++) {
                if (this.probability >= RANDOM.nextDouble())
                    newBulk++;
            }
            if (0 == newBulk) return false;
            traverser.asAdmin().setBulk(newBulk);
            return true;
        });
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.probability);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.BULK);
    }
}
