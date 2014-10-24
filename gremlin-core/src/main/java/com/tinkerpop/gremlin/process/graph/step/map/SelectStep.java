package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.Barrier;
import com.tinkerpop.gremlin.process.graph.marker.EngineDependent;
import com.tinkerpop.gremlin.process.graph.marker.PathConsumer;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.util.FunctionRing;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SelectStep<S, E> extends MapStep<S, Map<String, E>> implements PathConsumer, EngineDependent {

    private final FunctionRing functionRing;
    private final List<String> selectLabels;
    private final boolean wasEmpty;
    private boolean requiresPaths = false;
    private boolean onGraphComputer = false;
    protected Function<Traverser<S>, Map<String, E>> selectFunction;

    public SelectStep(final Traversal traversal, final List<String> selectLabels, final Function... stepFunctions) {
        super(traversal);
        this.functionRing = new FunctionRing(stepFunctions);
        this.wasEmpty = selectLabels.size() == 0;
        this.selectLabels = this.wasEmpty ? TraversalHelper.getLabelsUpTo(this, this.traversal) : selectLabels;
        this.selectFunction = traverser -> {
            final S start = traverser.get();
            final Map<String, E> bindings = new LinkedHashMap<>();

            if (this.requiresPaths && this.onGraphComputer) {   ////// PROCESS STEP BINDINGS
                final Path path = traverser.path();
                this.selectLabels.forEach(label -> {
                    if (path.hasLabel(label))
                        bindings.put(label, (E) this.functionRing.next().apply(path.get(label)));
                });
            } else {
                this.selectLabels.forEach(label -> { ////// PROCESS SIDE-EFFECTS
                    if (traverser.sideEffects().exists(label))
                        bindings.put(label, (E) this.functionRing.next().apply(traverser.get(label)));
                });
            }

            if (start instanceof Map) {  ////// PROCESS MAP BINDINGS
                if (this.wasEmpty)
                    ((Map) start).forEach((k, v) -> bindings.put((String) k, (E) this.functionRing.next().apply(v)));
                else
                    this.selectLabels.forEach(label -> {
                        if (((Map) start).containsKey(label)) {
                            bindings.put(label, (E) this.functionRing.next().apply(((Map) start).get(label)));
                        }
                    });
            }

            this.functionRing.reset();
            return bindings;
        };
        this.setFunction(this.selectFunction);
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

    public String toString() {
        return this.selectLabels.size() > 0 ?
                TraversalHelper.makeStepString(this, this.selectLabels) :
                TraversalHelper.makeStepString(this);
    }
}
