package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.PathConsumer;
import com.tinkerpop.gremlin.process.util.FunctionRing;
import com.tinkerpop.gremlin.util.function.SFunction;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SelectStep<E> extends MapStep<Object, Map<String, E>> implements PathConsumer {

    public final FunctionRing functionRing;
    public final String[] asLabels;

    public SelectStep(final Traversal traversal, final List<String> asLabels, SFunction... stepFunctions) {
        super(traversal);
        this.functionRing = new FunctionRing(stepFunctions);
        this.asLabels = asLabels.toArray(new String[asLabels.size()]);
        this.setFunction(traverser -> {
            final Path path = traverser.getPath();
            final Map<String, E> temp = new HashMap<>();
            if (this.functionRing.hasFunctions()) {
                if (this.asLabels.length == 0)
                    path.forEach((as, object) -> temp.put(as, (E) this.functionRing.next().apply(object)));
                else
                    path.subset(this.asLabels).forEach((as, object) -> temp.put(as, (E) this.functionRing.next().apply(object)));
                return temp;
            } else {
                if (this.asLabels.length == 0) {
                    path.forEach((k, v) -> temp.put(k, (E) v));
                } else {
                    for (final String as : this.asLabels) {
                        temp.put(as, path.get(as));
                    }
                }
            }
            return temp;
        });
    }
}
