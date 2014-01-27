package com.tinkerpop.gremlin.oltp.map;

import com.tinkerpop.gremlin.Path;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.util.FunctionRing;

import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ProjectPipe extends MapPipe<Path, Path> {

    public FunctionRing functionRing;

    public ProjectPipe(final Pipeline pipeline, final Function... projectionFunctions) {
        super(pipeline);
        this.functionRing = new FunctionRing(projectionFunctions);
        this.setFunction(holder -> {
            final Path path = holder.get();
            final Path temp = new Path();
            path.forEach((as, object) -> temp.add(as, this.functionRing.next().apply(object)));
            return temp;
        });
    }
}
