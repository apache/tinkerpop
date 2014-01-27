package com.tinkerpop.gremlin.oltp.map;

import com.tinkerpop.gremlin.Path;
import com.tinkerpop.gremlin.Pipeline;

import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ProjectPipe extends MapPipe<Path, Path> {

    public Function[] projections;
    public int currentProjection = 0;

    public ProjectPipe(final Pipeline pipeline, final Function... projectionFunctions) {
        super(pipeline);
        this.projections = projectionFunctions;
        this.setFunction(holder -> {
            final Path path = holder.get();
            final Path temp = new Path();
            path.forEach((as, object) -> {
                temp.add(as, this.projections[this.currentProjection].apply(object));
                this.currentProjection = (this.currentProjection + 1) % this.projections.length;
            });
            return temp;
        });
    }
}
