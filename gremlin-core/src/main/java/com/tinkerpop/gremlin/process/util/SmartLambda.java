package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.util.function.TraversableLambda;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SmartLambda<S, E> implements Function<S, E>, Predicate<S>, Consumer<S>, Cloneable, TraversableLambda<S,E> {

    private TraversalLambda<S, E> traversalLambda;
    private Object lambda;
    private boolean usesTraversalLambda;

    public SmartLambda() {
        this(new Function() {
            @Override
            public Object apply(final Object object) {
                return object;
            }

            @Override
            public String toString() {
                return "";
            }
        });
    }

    public SmartLambda(final Object lambda) {
        this.setLambda(lambda);
    }

    public void setLambda(final Object lambda) {
        if (lambda instanceof TraversalLambda) {
            this.traversalLambda = (TraversalLambda<S, E>) lambda;
            this.lambda = null;
        } else if (lambda instanceof TraversalObjectLambda) {
            this.traversalLambda = new TraversalLambda<>(((TraversalObjectLambda<S, E>) lambda).getTraversal());
            this.lambda = null;
        } else {
            this.traversalLambda = null;
            this.lambda = lambda;
        }
        this.usesTraversalLambda = this.traversalLambda != null;
    }

    // function
    @Override
    public E apply(final S traverser) {
        return this.usesTraversalLambda ?
                this.traversalLambda.apply((Traverser<S>) traverser) :
                ((Function<S, E>) this.lambda).apply(((Traverser<S>) traverser).get());
    }

    // predicate
    @Override
    public boolean test(final S traverser) {
        return this.usesTraversalLambda ? this.traversalLambda.test((Traverser<S>) traverser) : ((Predicate<S>) this.lambda).test(((Traverser<S>) traverser).get());
    }

    // consumer
    @Override
    public void accept(final S traverser) {
        if (this.usesTraversalLambda)
            this.traversalLambda.accept((Traverser<S>) traverser);
        else
            ((Consumer<S>) this.lambda).accept(((Traverser<S>) traverser).get());
    }

    @Override
    public String toString() {
        return this.usesTraversalLambda ? this.traversalLambda.toString() : this.lambda.toString();
    }

    @Override
    public SmartLambda<S, E> clone() throws CloneNotSupportedException {
        final SmartLambda<S, E> clone = (SmartLambda<S, E>) super.clone();
        if (clone.usesTraversalLambda)
            clone.traversalLambda = this.traversalLambda.clone();
        return clone;
    }

    public Set<TraverserRequirement> getRequirements() {
        return this.usesTraversalLambda ?
                this.traversalLambda.getTraversal().asAdmin().getTraverserRequirements() :
                Collections.singleton(TraverserRequirement.OBJECT);
    }

    @Override
    public Traversal<S, E> getTraversal() {
        return this.usesTraversalLambda ? this.traversalLambda.getTraversal() : EmptyTraversal.instance();
    }

    public List<Traversal<S, E>> getTraversalAsList() {
        return this.usesTraversalLambda ? Collections.singletonList(this.traversalLambda.getTraversal()) : Collections.emptyList();
    }
}