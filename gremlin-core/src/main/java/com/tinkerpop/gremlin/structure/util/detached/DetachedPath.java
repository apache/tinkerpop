package com.tinkerpop.gremlin.structure.util.detached;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DetachedPath extends Path {

    public DetachedPath() {

    }

    private DetachedPath(final Path path) {
        path.forEach((as, object) -> {
            if (object instanceof DetachedElement || object instanceof DetachedProperty) {
                this.add(as, object);
            } else if (object instanceof Vertex) {
                this.add(as, DetachedVertex.detach((Vertex) object));
            } else if (object instanceof Edge) {
                this.add(as, DetachedEdge.detach((Edge) object));
            } else if (object instanceof Property) {
                this.add(as, DetachedProperty.detach((Property) object));
            } else {
                this.add(as, object);
            }
        });
    }

    public Path attach(final Graph graph) {
        final Path path = new Path();
        this.forEach((as, object) -> {
            if (object instanceof DetachedVertex) {
                path.add(as, ((DetachedVertex) object).attach(graph));
            } else if (object instanceof DetachedEdge) {
                path.add(as, ((DetachedEdge) object).attach(graph));
            } else if (object instanceof DetachedProperty) {
                path.add(as, ((DetachedProperty) object).attach(graph));
            } else {
                path.add(as, object);
            }
        });
        return path;
    }

    public static DetachedPath detach(final Path path) {
        return new DetachedPath(path);
    }
}
