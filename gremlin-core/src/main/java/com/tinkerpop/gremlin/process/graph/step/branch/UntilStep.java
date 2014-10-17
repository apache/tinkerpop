package com.tinkerpop.gremlin.process.graph.step.branch;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.step.map.MapStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Compare;
import org.javatuples.Pair;

import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class UntilStep<S> extends MapStep<S, S> {

    private final String breakLabel;
    private final Pair<Short, Compare> jumpLoops;
    private final Predicate<Traverser<S>> breakPredicate;
    private final Predicate<Traverser<S>> emitPredicate;

    public UntilStep(Traversal traversal, final String breakLabel, final Predicate<Traverser<S>> breakPredicate, final Predicate<Traverser<S>> emitPredicate) {
        super(traversal);
        this.jumpLoops = null;
        this.breakLabel = breakLabel;
        this.breakPredicate = breakPredicate;
        this.emitPredicate = emitPredicate;
    }

    public UntilStep(Traversal traversal, final String breakLabel, final int loops, final Predicate<Traverser<S>> emitPredicate) {
        super(traversal);
        this.jumpLoops = Pair.with((short) loops, Compare.gt);
        this.breakLabel = breakLabel;
        this.breakPredicate = null;
        this.emitPredicate = emitPredicate;
    }

    public String toString() {
        return null != this.jumpLoops ?
                TraversalHelper.makeStepString(this, this.breakLabel, Compare.gt.asString() + this.jumpLoops.getValue0()) :
                TraversalHelper.makeStepString(this, this.breakLabel);
    }

    public String getBreakLabel() {
        return this.breakLabel;
    }

    public JumpStep<S> createLeftJumpStep(final Traversal traversal, final String jumpLabel) {
        final JumpStep.Builder<S> builder = JumpStep.<S>build(traversal).jumpLabel(jumpLabel);
        if (null != this.breakPredicate)
            builder.jumpPredicate(this.breakPredicate);
        else
            builder.jumpLoops(this.jumpLoops.getValue0(), this.jumpLoops.getValue1());
        if (null != this.emitPredicate)
            builder.emitPredicate(this.emitPredicate);
        else
            builder.emitChoice(false);
        return builder.create();
    }

    public JumpStep<S> createRightJumpStep(final Traversal traversal, final String jumpLabel) {
        return JumpStep.<S>build(traversal).jumpLabel(jumpLabel).jumpChoice(true).emitChoice(false).create();
    }
}
