package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Compare;

import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class UntilStep<S> extends MapStep<S, S> {

    public final String breakLabel;
    public final int loops;
    public final Predicate<Traverser<S>> breakPredicate;
    public final Predicate<Traverser<S>> emitPredicate;

    public UntilStep(Traversal traversal, final String breakLabel, final Predicate<Traverser<S>> breakPredicate, final Predicate<Traverser<S>> emitPredicate) {
        super(traversal);
        this.loops = -1;
        this.breakLabel = breakLabel;
        this.breakPredicate = breakPredicate;
        this.emitPredicate = emitPredicate;
    }

    public UntilStep(Traversal traversal, final String breakLabel, final int loops, final Predicate<Traverser<S>> emitPredicate) {
        super(traversal);
        this.loops = loops;
        this.breakLabel = breakLabel;
        this.breakPredicate = null;
        this.emitPredicate = emitPredicate;
    }

    public String toString() {
        return this.loops != -1 ?
                TraversalHelper.makeStepString(this, this.breakLabel, Compare.GREATER_THAN.asString() + this.loops) :
                TraversalHelper.makeStepString(this, this.breakLabel);
    }
}
