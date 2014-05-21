package com.tinkerpop.gremlin.process.graph.map;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.FunctionRing;
import com.tinkerpop.gremlin.process.util.PathConsumer;
import com.tinkerpop.gremlin.util.function.SFunction;

import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SelectStep extends MapStep<Object, Path> implements PathConsumer {

    public final FunctionRing functionRing;
    public final String[] asLabels;

    public SelectStep(final Traversal traversal, final List<String> asLabels, SFunction... stepFunctions) {
        super(traversal);
        this.functionRing = new FunctionRing(stepFunctions);
        this.asLabels = asLabels.toArray(new String[asLabels.size()]);
        this.setFunction(traverser -> {
            final Path path = traverser.getPath();
            if (this.functionRing.hasFunctions()) {
                final Path temp = new Path();
                if (this.asLabels.length == 0)
                    path.forEach((as, object) -> temp.add(as, this.functionRing.next().apply(object)));
                else
                    path.subset(this.asLabels).forEach((as, object) -> temp.add(as, this.functionRing.next().apply(object)));
                return temp;
            } else {
                return this.asLabels.length == 0 ? path : path.subset(this.asLabels);
            }
        });
    }
}
