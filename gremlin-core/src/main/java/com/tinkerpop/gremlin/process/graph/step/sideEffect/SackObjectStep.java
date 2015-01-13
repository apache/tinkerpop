package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SackObjectStep<S, V> extends SideEffectStep<S> {

    private static final Set<TraverserRequirement> REQUIREMENTS = new HashSet<>(Arrays.asList(
            TraverserRequirement.SACK,
            TraverserRequirement.OBJECT
    ));

    private final BiFunction<V, S, V> operator;

    public SackObjectStep(final Traversal traversal, final BiFunction<V, S, V> operator) {
        super(traversal);
        this.operator = operator;
        this.setConsumer(traverser -> traverser.sack(this.operator.apply(traverser.sack(), traverser.get())));
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return REQUIREMENTS;
    }
}
