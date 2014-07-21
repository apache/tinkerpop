package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.util.EmptyIterator;
import com.tinkerpop.gremlin.process.util.SingleIterator;
import com.tinkerpop.gremlin.util.function.SFunction;
import com.tinkerpop.gremlin.util.function.SPredicate;

import java.util.Map;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BranchStep<S, E, M> extends FlatMapStep<S, E> {

    public BranchStep(final Traversal traversal,
                      final SPredicate<Traverser<S>> ifFunction,
                      final Traversal<S, E> trueBranch,
                      final Traversal<S, E> falseBranch) {
        super(traversal);

        this.setFunction(traverser -> {

            boolean choice = null == ifFunction
                    ? toBoolean(traverser.get())
                    : ifFunction.test(traverser);

            final Traversal<S, E> t = choice ? trueBranch : falseBranch;
            t.addStarts(new SingleIterator<>(traverser));
            return t;
        });
    }

    public BranchStep(final Traversal traversal,
                      final SFunction<Traverser<S>, M> mapFunction,
                      final Map<M, Traversal<S, E>> choices) {
        super(traversal);

        this.setFunction(traverser -> {
            M basis = null == mapFunction
                    ? (M) traverser.get()
                    : mapFunction.apply(traverser);
            final Traversal<S, E> choice = choices.get(basis);
            if (null == choice) {
                return new EmptyIterator<>();
            } else {
                choice.addStarts(new SingleIterator<>(traverser));
                return choice;
            }
        });
    }

    private boolean toBoolean(S s) {
        return s instanceof Boolean && s.equals(true);
    }
}
