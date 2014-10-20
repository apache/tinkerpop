package com.tinkerpop.gremlin.process.graph.step.branch;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.EngineDependent;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.EmptyTraverser;
import com.tinkerpop.gremlin.process.util.FunctionRing;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.process.util.TraverserSet;
import com.tinkerpop.gremlin.structure.Compare;
import com.tinkerpop.gremlin.structure.Graph;

import java.util.function.Function;
import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BranchStep<S> extends AbstractStep<S, S> implements EngineDependent {

    public static final String EMPTY_LABEL = Graph.System.system("emptyLabel");
    public static final String THIS_LABEL = Graph.System.system("thisLabel");
    public static final String THIS_BREAK_LABEL = Graph.System.system("thisBreakLabel");

    private FunctionRing<Traverser<S>, String> functionRing;
    private boolean onGraphComputer = false;
    private TraverserSet<S> graphComputerQueue;

    public BranchStep(final Traversal traversal) {
        super(traversal);
        this.futureSetByChild = true;
    }

    public void setFunctions(final Function<Traverser<S>, String>... labelFunctions) {
        this.functionRing = new FunctionRing<>(labelFunctions);
    }

    @Override
    public Traverser<S> processNextStart() {
        return this.onGraphComputer ? this.computerAlgorithm() : this.standardAlgorithm();
    }

    private final Traverser<S> computerAlgorithm() {
        while (true) {
            if (!this.graphComputerQueue.isEmpty())
                return this.graphComputerQueue.remove();
            final Traverser<S> traverser = this.starts.next();
            while (!this.functionRing.roundComplete()) {
                final String goTo = this.functionRing.next().apply(traverser);
                if (THIS_BREAK_LABEL.equals(goTo)) {
                    final Traverser.Admin<S> sibling = traverser.asAdmin().makeSibling();
                    sibling.setFuture(this.getNextStep().getLabel());
                    sibling.resetLoops();
                    this.graphComputerQueue.add(sibling);
                    break;
                } else if (THIS_LABEL.equals(goTo)) {
                    final Traverser.Admin<S> sibling = traverser.asAdmin().makeSibling();
                    sibling.setFuture(this.getNextStep().getLabel());
                    sibling.resetLoops();
                    this.graphComputerQueue.add(sibling);
                } else if (!EMPTY_LABEL.equals(goTo)) {
                    final Traverser.Admin<S> sibling = traverser.asAdmin().makeSibling();
                    if (TraversalHelper.relativeLabelDirection(this, goTo) == -1)
                        sibling.incrLoops();
                    sibling.setFuture(goTo);
                    this.graphComputerQueue.add(sibling);
                }
            }
            this.functionRing.reset();
        }
    }

    private final Traverser<S> standardAlgorithm() {
        final Traverser<S> traverser = this.starts.next();
        while (!this.functionRing.roundComplete()) {
            // TODO: if its a jumpBack, you have to incr the loop prior to the function execution.
            // TODO: but you don't know if the label is jump back until you execute the function.
            final String goTo = this.functionRing.next().apply(traverser);
            if (THIS_BREAK_LABEL.equals(goTo)) {
                final Traverser.Admin<S> sibling = traverser.asAdmin().makeSibling();
                sibling.resetLoops();
                sibling.setFuture(this.getNextStep().getLabel());
                this.getNextStep().addStart(sibling);
                break;
            } else if (THIS_LABEL.equals(goTo)) {
                final Traverser.Admin<S> sibling = traverser.asAdmin().makeSibling();
                sibling.resetLoops();
                sibling.setFuture(this.getNextStep().getLabel());
                this.getNextStep().addStart(sibling);
            } else if (!EMPTY_LABEL.equals(goTo)) {
                final Traverser.Admin<S> sibling = traverser.asAdmin().makeSibling();
                if (TraversalHelper.relativeLabelDirection(this, goTo) == -1)
                    sibling.incrLoops();
                sibling.setFuture(goTo);
                TraversalHelper.<S, Object>getStep(goTo, this.getTraversal())
                        .getNextStep()
                        .addStart((Traverser) sibling);
            }
        }
        this.functionRing.reset();
        return EmptyTraverser.instance();
    }

    @Override
    public void onEngine(final Engine engine) {
        if (engine.equals(Engine.COMPUTER)) {
            this.onGraphComputer = true;
            this.graphComputerQueue = new TraverserSet<>();
        } else {
            this.onGraphComputer = false;
            this.graphComputerQueue = null;
        }
    }

    /*@Override
    public String toString() {
      // TODO
    }*/

    public static class GoToLabel<S> implements Function<Traverser<S>, String> {

        private final String goToLabel;

        public GoToLabel(final String goToLabel) {
            this.goToLabel = goToLabel;
        }

        public String apply(final Traverser<S> traverser) {
            return this.goToLabel;
        }
    }

    public static class GoToLabelWithLoops<S> implements Function<Traverser<S>, String> {

        private final String goToLabel;
        private final String breakLabel;
        private final Compare loopCompare;
        private final short loops;

        public GoToLabelWithLoops(final String goToLabel, final Compare loopCompare, final short loops, final String breakLabel) {
            this.goToLabel = goToLabel;
            this.breakLabel = breakLabel;
            this.loops = loops;
            this.loopCompare = loopCompare;
        }

        public String apply(final Traverser<S> traverser) {
            return this.loopCompare.test(traverser.loops(), this.loops) ? this.goToLabel : this.breakLabel;
        }
    }

    public static class GoToLabelWithPredicate<S> implements Function<Traverser<S>, String> {

        private final String goToLabel;
        private final String breakLabel;
        private final Predicate<Traverser<S>> goToPredicate;

        public GoToLabelWithPredicate(final String goToLabel, final Predicate<Traverser<S>> goToPredicate, final String breakLabel) {
            this.goToLabel = goToLabel;
            this.breakLabel = breakLabel;
            this.goToPredicate = goToPredicate;
        }

        public String apply(final Traverser<S> traverser) {
            return this.goToPredicate.test(traverser) ? this.goToLabel : this.breakLabel;

        }
    }
}
