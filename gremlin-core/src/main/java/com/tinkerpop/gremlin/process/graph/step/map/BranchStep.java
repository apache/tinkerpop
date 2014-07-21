package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.util.EmptyIterator;
import com.tinkerpop.gremlin.process.util.SingleIterator;
import com.tinkerpop.gremlin.util.function.SFunction;

import java.util.Map;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class BranchStep<S, E> extends FlatMapStep<S, E> {

    public BranchStep(final Traversal traversal,
                      final SFunction<Traverser<S>, Boolean> mapFunction,
                      final Traversal<S, E> trueBranch,
                      final Traversal<S, E> falseBranch) {
        super(traversal);

        this.setFunction(traverser -> {
            boolean choice = null == mapFunction
                    ? toBoolean(traverser.get())
                    : mapFunction.apply(traverser);
            Traversal<S, E> t = choice ? trueBranch : falseBranch;

            t.addStarts(new SingleIterator<>(traverser));
            return t;
        });
    }

    public BranchStep(final Traversal traversal,
                      final SFunction<Traverser<S>, S> mapFunction,
                      final Map<S, Traversal<S, E>> choices) {
        super(traversal);

        this.setFunction(traverser -> {
            S basis = null == mapFunction
                    ? traverser.get()
                    : mapFunction.apply(traverser);
            Traversal<S, E> choice = choices.get(basis);

            if (null == choice) {
                return new EmptyIterator<>();
            } else {
                choice.addStarts(new SingleIterator<>(traverser));
                return choice;
            }
        });
    }

    /*
    public BranchStep(final Traversal traversal,
                      final SFunction<Traverser<S>, S> mapFunction,
                      final Map<S, SFunction<Traverser<S>, E>> choices) {
        super(traversal);

        this.setFunction(traverser -> {
            S basis = null == mapFunction
                    ? traverser.get()
                    : mapFunction.apply(traverser);
            System.out.println("basis = " + basis + " (" + basis.getClass() + ")");
            SFunction<Traverser<S>, E> func = choices.get(basis);
            return null == func ? new EmptyIterator<>() : new SingleIterator<>(func.apply(traverser));
        });
    }

    public BranchStep(final Traversal traversal,
                      final SFunction<Traverser<S>, S> mapFunction,
                      final Map<SFunction<S, Boolean>, Traversal<S, E>> choices,
                      final boolean condIgnore) {
        super(traversal);

        this.setFunction(traverser -> {
            S basis = null == mapFunction
                    ? traverser.get()
                    : mapFunction.apply(traverser);

            Traversal<S, E> choice = null;
            if (null != basis) {
                for (Map.Entry<SFunction<S, Boolean>, Traversal<S, E>> e : choices.entrySet()) {
                    if (e.getKey().apply(basis)) {
                        choice = e.getValue();
                        break;
                    }
                }
            }

            if (null == choice) {
                return new EmptyIterator<>();
            } else {
                choice.addStarts(new SingleIterator<>(traverser));
                return choice;
            }
        });
    }
    */

    private boolean toBoolean(S s) {
        return s instanceof Boolean && s.equals(true);
    }
}
