package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.EngineDependent;
import com.tinkerpop.gremlin.process.graph.marker.FunctionHolder;
import com.tinkerpop.gremlin.process.graph.marker.TraversalHolder;
import com.tinkerpop.gremlin.process.graph.step.util.CollectingBarrierStep;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.FunctionRing;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.util.function.TraversableLambda;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SelectStep<S, E> extends MapStep<S, Map<String, E>> implements FunctionHolder<Object, Object>, TraversalHolder, EngineDependent {

    protected FunctionRing<Object, Object> functionRing;
    private final List<String> selectLabels;
    private final boolean wasEmpty;
    private boolean requiresPaths = false;
    protected Function<Traverser<S>, Map<String, E>> selectFunction;

    public SelectStep(final Traversal traversal, final String... selectLabels) {
        super(traversal);
        this.functionRing = new FunctionRing<>();
        this.wasEmpty = selectLabels.length == 0;
        this.selectLabels = this.wasEmpty ? TraversalHelper.getLabelsUpTo(this, this.traversal.asAdmin()) : Arrays.asList(selectLabels);
        SelectStep.generateFunction(this);
    }

    @Override
    public void reset() {
        super.reset();
        this.functionRing.reset();
    }

    @Override
    public void onEngine(final TraversalEngine traversalEngine) {
        this.requiresPaths = traversalEngine.equals(TraversalEngine.COMPUTER) ?
                TraversalHelper.getLabelsUpTo(this, this.traversal.asAdmin()).stream().filter(this.selectLabels::contains).findAny().isPresent() :
                TraversalHelper.getStepsUpTo(this, this.traversal.asAdmin()).stream()
                        .filter(step -> step instanceof CollectingBarrierStep)
                        .filter(step -> TraversalHelper.getLabelsUpTo(step, this.traversal.asAdmin()).stream().filter(this.selectLabels::contains).findAny().isPresent()
                                || (step.getLabel().isPresent() && this.selectLabels.contains(step.getLabel().get()))) // TODO: get rid of this (there is a test case to check it)
                        .findAny().isPresent();
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.selectLabels, this.functionRing);
    }

    @Override
    public SelectStep<S, E> clone() throws CloneNotSupportedException {
        final SelectStep<S, E> clone = (SelectStep<S, E>) super.clone();
        clone.functionRing = this.functionRing.clone();
        for (final Traversal<Object, Object> traversal : clone.functionRing.getTraversals()) {
            clone.executeTraversalOperations(traversal, TYPICAL_LOCAL_OPERATIONS);
        }
        SelectStep.generateFunction(clone);
        return clone;
    }

    @Override
    public void addFunction(final Function<Object, Object> function) {
        this.functionRing.addFunction(function);
        if (function instanceof TraversableLambda)
            this.executeTraversalOperations(((TraversableLambda) function).getTraversal(), TYPICAL_LOCAL_OPERATIONS);
    }

    @Override
    public List<Function<Object, Object>> getFunctions() {
        return this.functionRing.getFunctions();
    }

    @Override
    public List<Traversal<Object, Object>> getLocalTraversals() {
        return this.functionRing.getTraversals();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        final Set<TraverserRequirement> requirements = TraversalHolder.super.getRequirements();
        if (this.requiresPaths)
            requirements.add(TraverserRequirement.PATH);
        requirements.add(TraverserRequirement.OBJECT);
        requirements.add(TraverserRequirement.PATH_ACCESS);
        return requirements;
    }

    //////////////////////

    private static final <S, E> void generateFunction(final SelectStep<S, E> selectStep) {
        selectStep.selectFunction = traverser -> {
            final S start = traverser.get();
            final Map<String, E> bindings = new LinkedHashMap<>();

            ////// PROCESS STEP BINDINGS
            final Path path = traverser.path();
            selectStep.selectLabels.forEach(label -> {
                if (path.hasLabel(label))
                    bindings.put(label, (E) selectStep.functionRing.next().apply(path.get(label)));
            });

            ////// PROCESS MAP BINDINGS
            if (start instanceof Map) {
                if (selectStep.wasEmpty)
                    ((Map) start).forEach((k, v) -> bindings.put((String) k, (E) selectStep.functionRing.next().apply(v)));
                else
                    selectStep.selectLabels.forEach(label -> {
                        if (((Map) start).containsKey(label)) {
                            bindings.put(label, (E) selectStep.functionRing.next().apply(((Map) start).get(label)));
                        }
                    });
            }

            selectStep.functionRing.reset();
            return bindings;
        };
        selectStep.setFunction(selectStep.selectFunction);
    }
}
