package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.EngineDependent;
import com.tinkerpop.gremlin.process.graph.marker.PathConsumer;
import com.tinkerpop.gremlin.process.util.FunctionRing;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.util.function.SFunction;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SelectStep<S, E> extends MapStep<S, Map<String, E>> implements PathConsumer, EngineDependent {

    public final FunctionRing functionRing;
    public final List<String> selectLabels;
    private final boolean wasEmpty;
    private boolean requiresPaths = false;

    public SelectStep(final Traversal traversal, final List<String> selectLabels, SFunction... stepFunctions) {
        super(traversal);
        this.functionRing = new FunctionRing(stepFunctions);
        this.wasEmpty = selectLabels.size() == 0;
        this.selectLabels = this.wasEmpty ? TraversalHelper.getLabelsUpTo(this, this.traversal) : selectLabels;
        this.setFunction(traverser -> {
            final S start = traverser.get();
            final Map<String, E> bindings = new LinkedHashMap<>();

            if (this.requiresPaths) {   ////// PROCESS STEP BINDINGS
                final Path path = traverser.getPath();
                this.selectLabels.forEach(label -> {
                    if (path.hasLabel(label))
                        bindings.put(label, (E) this.functionRing.next().apply(path.get(label)));
                });
            } else {
                this.selectLabels.forEach(label -> {
                    if (traverser.getSideEffects().exists(label))
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
        });
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
    public void onEngine(final Engine engine) {
        if (engine.equals(Engine.COMPUTER))
            this.requiresPaths = TraversalHelper.getLabelsUpTo(this, this.traversal).stream().filter(label -> this.selectLabels.contains(label)).findFirst().isPresent();
        else
            this.requiresPaths = false;
    }

    public String toString() {
        return this.selectLabels.size() > 0 ?
                TraversalHelper.makeStepString(this, this.selectLabels) :
                TraversalHelper.makeStepString(this);
    }
}
