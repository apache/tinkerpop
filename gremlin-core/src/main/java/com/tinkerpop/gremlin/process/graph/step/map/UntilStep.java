package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.util.function.SPredicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class UntilStep<S> extends MapStep<S, S> {

    public final String jumpLabel;
    public final SPredicate<Traverser<S>> jumpPredicate;
    public final SPredicate<Traverser<S>> emitPredicate;

    public UntilStep(Traversal traversal, final String jumpLabel, final SPredicate<Traverser<S>> jumpPredicate, final SPredicate<Traverser<S>> emitPredicate) {
        super(traversal);
        this.jumpLabel = jumpLabel;
        this.jumpPredicate = jumpPredicate;
        this.emitPredicate = emitPredicate;

    }
}
