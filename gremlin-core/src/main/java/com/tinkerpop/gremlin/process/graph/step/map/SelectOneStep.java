package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.util.function.SFunction;

import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SelectOneStep<E> extends MapStep<Object, E> {

    public final String selectLabel;
    public final SFunction stepFunction;

    public SelectOneStep(final Traversal traversal, final String selectLabel, SFunction stepFunction) {
        super(traversal);
        this.selectLabel = selectLabel;
        this.stepFunction = stepFunction;
        this.setFunction(traverser -> {
            final Path path = traverser.hasPath() ? traverser.getPath() : null;
            final Object start = traverser.get();
            if (null != this.stepFunction) {
                if (null != path && path.hasLabel(selectLabel))
                    return (E) this.stepFunction.apply(path.get(selectLabel));
                if (start instanceof Map && ((Map) start).containsKey(selectLabel))
                    return (E) this.stepFunction.apply(((Map) start).get(selectLabel));
            } else {
                if (null != path && path.hasLabel(selectLabel))
                    return path.get(selectLabel);
                if (start instanceof Map && ((Map) start).containsKey(selectLabel))
                    return (E) ((Map) start).get(selectLabel);
            }
            return (E) NO_OBJECT;
        });
    }

    public boolean hasStepFunction() {
        return null != this.stepFunction;
    }

    public String toString() {
        return TraversalHelper.makeStepString(this, this.selectLabel);
    }
}
