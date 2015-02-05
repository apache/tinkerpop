package com.tinkerpop.gremlin.process.graph.traversal.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.traversal.step.Reversible;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.Collections;
import java.util.Set;
import java.util.function.BiPredicate;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public final class IsStep<S> extends FilterStep<S> implements Reversible {

    private final Object value;
    private final BiPredicate<S,Object> predicate;

    public IsStep(final Traversal.Admin traversal, final BiPredicate<S, Object> predicate, final Object value) {
        super(traversal);
        this.value = value;
        this.predicate = predicate;
        this.setPredicate(traverser -> this.predicate.test(traverser.get(), this.value));
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.predicate, this.value);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }
}
