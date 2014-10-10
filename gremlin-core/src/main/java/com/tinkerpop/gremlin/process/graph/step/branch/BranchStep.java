package com.tinkerpop.gremlin.process.graph.step.branch;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.EmptyTraverser;
import com.tinkerpop.gremlin.process.util.FunctionRing;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Graph;

import java.util.function.Function;
import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BranchStep<S> extends AbstractStep<S, S> {

    private FunctionRing<Traverser<S>, String> functionRing;
    public static final String EMPTY_LABEL = Graph.System.system("emptyLabel");

    public BranchStep(final Traversal traversal) {
        super(traversal);
    }

    public void setFunctions(final Function<Traverser<S>, String>... labelFunctions) {
        this.functionRing = new FunctionRing<>(labelFunctions);
    }

    @Override
    public Traverser<S> processNextStart() {
        final Traverser<S> traverser = this.starts.next();
        this.functionRing.reset();
        while (!this.functionRing.roundComplete()) {
            final String goTo = this.functionRing.next().apply(traverser);
            if (EMPTY_LABEL.equals(goTo)) {
                TraversalHelper.<S, Object>getStep(goTo, this.getTraversal())
                        .getNextStep()
                        .addStart((Traverser) traverser.asAdmin().makeSibling());
            }
        }
        return EmptyTraverser.instance();
    }

    public static class GoToLabel<S> implements Function<Traverser<S>, String> {

        private final String goToLabel;

        public GoToLabel(final String goToLabel) {
            this.goToLabel = goToLabel;
        }

        public String apply(final Traverser<S> traverser) {
            return this.goToLabel;
        }
    }

    public static class GoToLabelWithPredicate<S> implements Function<Traverser<S>, String> {

        private final String goToLabel;
        private final Predicate<Traverser<S>> goToPredicate;

        public GoToLabelWithPredicate(final String goToLabel, final Predicate<Traverser<S>> goToPredicate) {
            this.goToLabel = goToLabel;
            this.goToPredicate = goToPredicate;
        }

        public String apply(final Traverser<S> traverser) {
            return this.goToPredicate.test(traverser) ? this.goToLabel : EMPTY_LABEL;

        }
    }
}
