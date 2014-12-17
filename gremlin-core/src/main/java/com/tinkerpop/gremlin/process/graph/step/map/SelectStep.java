package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.Barrier;
import com.tinkerpop.gremlin.process.graph.marker.EngineDependent;
import com.tinkerpop.gremlin.process.graph.marker.FunctionHolder;
import com.tinkerpop.gremlin.process.graph.marker.PathConsumer;
import com.tinkerpop.gremlin.process.util.FunctionRing;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SelectStep<S, E> extends MapStep<S, Map<String, E>> implements PathConsumer, FunctionHolder<Object, Object>, EngineDependent {

    protected FunctionRing<Object, Object> functionRing;
    private final List<String> selectLabels;
    private final boolean wasEmpty;
    private boolean requiresPaths = false;
    private boolean onGraphComputer = false;
    protected Function<Traverser<S>, Map<String, E>> selectFunction;

    public SelectStep(final Traversal traversal, final String... selectLabels) {
        super(traversal);
        this.functionRing = new FunctionRing<>();
        this.wasEmpty = selectLabels.length == 0;
        this.selectLabels = this.wasEmpty ? TraversalHelper.getLabelsUpTo(this, this.traversal) : Arrays.asList(selectLabels);
        SelectStep.generateFunction(this);
    }

    @Override
    public void reset() {
        super.reset();
        this.functionRing.reset();
    }

    public boolean hasStepFunctions() {
        return this.functionRing.hasFunctions();
    }

    @Override
    public boolean requiresPaths() {
        return this.requiresPaths;
    }

    @Override
    public void onEngine(final TraversalEngine traversalEngine) {
        this.onGraphComputer = traversalEngine.equals(TraversalEngine.COMPUTER);
        this.requiresPaths = traversalEngine.equals(TraversalEngine.COMPUTER) ?
                TraversalHelper.getLabelsUpTo(this, this.traversal).stream().filter(this.selectLabels::contains).findAny().isPresent() :
                TraversalHelper.getStepsUpTo(this, this.traversal).stream()
                        .filter(step -> step instanceof Barrier)
                        .filter(step -> TraversalHelper.getLabelsUpTo(step, this.traversal).stream().filter(this.selectLabels::contains).findAny().isPresent())
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
        SelectStep.generateFunction(clone);
        return clone;
    }

    @Override
    public void addFunction(final Function<Object, Object> function) {
        this.functionRing.addFunction(function);
    }

    @Override
    public List<Function<Object, Object>> getFunctions() {
        return this.functionRing.getFunctions();
    }

    //////////////////////

    private static final <S, E> void generateFunction(final SelectStep<S, E> selectStep) {
        selectStep.selectFunction = traverser -> {
            final S start = traverser.get();
            final Map<String, E> bindings = new LinkedHashMap<>();

            if (selectStep.requiresPaths && selectStep.onGraphComputer) {   ////// PROCESS STEP BINDINGS
                final Path path = traverser.path();
                selectStep.selectLabels.forEach(label -> {
                    if (path.hasLabel(label))
                        bindings.put(label, (E) selectStep.functionRing.next().apply(path.get(label)));
                });
            } else {
                selectStep.selectLabels.forEach(label -> { ////// PROCESS SIDE-EFFECTS
                    if (traverser.sideEffects().exists(label))
                        bindings.put(label, (E) selectStep.functionRing.next().apply(traverser.get(label)));
                });
            }

            if (start instanceof Map) {  ////// PROCESS MAP BINDINGS
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
