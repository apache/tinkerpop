package com.tinkerpop.gremlin.process.graph.step.branch;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.EngineDependent;
import com.tinkerpop.gremlin.process.graph.marker.StrategyProvider;
import com.tinkerpop.gremlin.process.graph.step.map.FlatMapStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * A step which offers a choice of two or more Traversals to take
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ChooseMapStep<S, E, M> extends FlatMapStep<S, E> implements StrategyProvider {

    private final Function<Traverser<S>, M> mapFunction;
    private final Map<M, Traversal<S, E>> branches;


    public ChooseMapStep(final Traversal traversal, final Function<Traverser<S>, M> mapFunction, final Map<M, Traversal<S, E>> branches) {
        super(traversal);
        this.mapFunction = mapFunction;
        this.branches = branches;
        this.setFunction(traverser -> {
            final Traversal<S, E> branch = this.branches.get(this.mapFunction.apply(traverser));
            if (null == branch) {
                return Collections.emptyIterator();
            } else {
                branch.addStart(traverser);
                return branch;
            }
        });
    }

    @Override
    public List<TraversalStrategy> getStrategies(final EngineDependent.Engine engine) {
        return engine.equals(EngineDependent.Engine.COMPUTER) ? Arrays.asList(ChooseMapStrategy.instance()) : Collections.emptyList();
    }

    public static class ChooseMapStrategy implements TraversalStrategy.NoDependencies {

        private static final ChooseMapStrategy INSTANCE = new ChooseMapStrategy();

        private ChooseMapStrategy() {
        }

        // x.choose(t -> M){a}{b}.y
        // x.branch(mapFunction.next().toString()).a.branch(end).as(z).b.as(end).y
        public void apply(final Traversal<?, ?> traversal) {
            TraversalHelper.getStepsOfClass(ChooseMapStep.class, traversal).forEach(step -> {
                final BranchStep<?> branchStep = new BranchStep<>(traversal);
                branchStep.setFunctions(traverser -> step.mapFunction.apply(traverser).toString()); // TODO: include class type
                TraversalHelper.replaceStep(step, branchStep, traversal);

                Step currentStep = branchStep;
                for (final Map.Entry<?, Traversal<?, ?>> entry : (Set<Map.Entry>) step.branches.entrySet()) {
                    currentStep.setLabel(entry.getKey().toString());   // TODO: include class type
                    for (final Step mapStep : entry.getValue().getSteps()) {
                        TraversalHelper.insertAfterStep(mapStep, currentStep, traversal);
                        currentStep = mapStep;
                    }
                    final BranchStep breakStep = new BranchStep(traversal);
                    breakStep.setFunctions(new BranchStep.GoToLabel("end"));
                    TraversalHelper.insertAfterStep(breakStep, currentStep, traversal);
                    currentStep = breakStep;
                }

                final IdentityStep finalStep = new IdentityStep(traversal);
                finalStep.setLabel("end");
                TraversalHelper.insertAfterStep(finalStep, currentStep, traversal);

            });
        }

        public static ChooseMapStrategy instance() {
            return INSTANCE;
        }
    }
}
