package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.FunctionRing;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.util.function.SFunction;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SelectStep<E> extends MapStep<Object, Map<String, E>> {

    public final FunctionRing functionRing;
    public final List<String> selectLabels;
    private final boolean wasEmpty;

    public SelectStep(final Traversal traversal, final List<String> selectLabels, SFunction... stepFunctions) {
        super(traversal);
        this.functionRing = new FunctionRing(stepFunctions);
        this.wasEmpty = selectLabels.size() == 0;
        this.selectLabels = this.wasEmpty ? TraversalHelper.getLabels(this.traversal) : selectLabels;
        this.setFunction(traverser -> {
            final Path path = traverser.hasPath() ? traverser.getPath() : null;
            final Object start = traverser.get();
            final Map<String, E> temp = new LinkedHashMap<>();
            if (this.functionRing.hasFunctions()) {   ////////// FUNCTION RING
                if (null != path)
                    this.selectLabels.forEach(as -> {   ////// PROCESS PATHS
                        if (path.hasLabel(as))
                            temp.put(as, (E) this.functionRing.next().apply(path.get(as)));
                    });

                if (start instanceof Map) {  ////// PROCESS MAPS
                    if (this.wasEmpty)
                        ((Map) start).forEach((k, v) -> temp.put((String) k, (E) this.functionRing.next().apply(v)));
                    else
                        this.selectLabels.forEach(as -> {
                            if (((Map) start).containsKey(as))
                                temp.put(as, (E) this.functionRing.next().apply(((Map) start).get(as)));
                        });
                }
            } else {    ////////// IF NO FUNCTION RING
                if (path != null)
                    this.selectLabels.forEach(as -> {  ////// PROCESS PATHS
                        if (path.hasLabel(as))
                            temp.put(as, path.get(as));
                    });
                if (start instanceof Map) {  ////// PROCESS MAPS
                    if (this.wasEmpty)
                        ((Map) start).forEach((k, v) -> temp.put((String) k, (E) v));
                    else
                        this.selectLabels.forEach(as -> {
                            if (((Map) start).containsKey(as))
                                temp.put(as, (E) ((Map) start).get(as));
                        });
                }
            }
            this.functionRing.reset();
            return temp;
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

    public String toString() {
        return this.selectLabels.size() > 0 ?
                TraversalHelper.makeStepString(this, this.selectLabels) :
                TraversalHelper.makeStepString(this);
    }
}
