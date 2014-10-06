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
    public final short loops;
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
        this.loops = (short) loops;
        this.breakLabel = breakLabel;
        this.breakPredicate = null;
        this.emitPredicate = emitPredicate;
    }

    public String toString() {
        return this.loops != -1 ?
                TraversalHelper.makeStepString(this, this.breakLabel, Compare.gt.asString() + this.loops) :
                TraversalHelper.makeStepString(this, this.breakLabel);
    }

    public JumpStep<S> createLeftJumpStep(final Traversal traversal, final String jumpLabel) {
        final JumpStep.Builder<S> builder = JumpStep.<S>build(traversal).jumpLabel(jumpLabel);
        if(null != this.breakPredicate)
            builder.jumpPredicate(this.breakPredicate);
        else
            builder.jumpLoops((int)this.loops, Compare.gt);
        if(null != this.emitPredicate)
            builder.emitPredicate(this.emitPredicate);
        else
            builder.emitChoice(false);
       return builder.create();
    }
}
