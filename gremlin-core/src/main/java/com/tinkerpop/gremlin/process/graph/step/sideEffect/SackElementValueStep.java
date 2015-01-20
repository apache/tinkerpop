package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Element;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BinaryOperator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SackElementValueStep<S extends Element, V> extends SideEffectStep<S> {

    private static final Set<TraverserRequirement> REQUIREMENTS = new HashSet<>(Arrays.asList(
            TraverserRequirement.SACK,
            TraverserRequirement.OBJECT
    ));

    private final BinaryOperator<V> operator;
    private final String propertyKey;

    public SackElementValueStep(final Traversal traversal, final BinaryOperator<V> operator, final String propertyKey) {
        super(traversal);
        this.operator = operator;
        this.propertyKey = propertyKey;
        this.setConsumer(traverser -> {
            traverser.get().iterators().valueIterator(this.propertyKey).forEachRemaining(value -> {
                traverser.sack(this.operator.apply(traverser.sack(), (V) value));
            });
        });
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.operator, this.propertyKey);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return REQUIREMENTS;
    }
}
