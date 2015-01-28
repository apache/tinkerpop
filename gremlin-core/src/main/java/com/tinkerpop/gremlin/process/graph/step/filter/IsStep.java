package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Collections;
import java.util.Set;
import java.util.function.BiPredicate;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class IsStep<S> extends FilterStep<S> implements Reversible {

    private final Object value;

    public IsStep(final Traversal traversal, final BiPredicate predicate, final Object value) {
        super(traversal);
        this.value = value;
        this.setPredicate(traverser -> predicate.test(traverser.get(), this.value));
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.value);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.BULK);
    }
}
