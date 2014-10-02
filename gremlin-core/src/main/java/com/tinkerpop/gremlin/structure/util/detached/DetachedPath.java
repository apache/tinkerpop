package com.tinkerpop.gremlin.structure.util.detached;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.util.DefaultMutablePath;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DetachedPath extends DefaultMutablePath {

    public DetachedPath() {

    }

    private DetachedPath(final Path path) {
        path.forEach((as, object) -> {
            if (object instanceof DetachedElement || object instanceof DetachedProperty) {
                this.objects.add(object);
                this.labels.add(as);
            } else if (object instanceof Vertex) {
                this.objects.add(DetachedVertex.detach((Vertex) object));
                this.labels.add(as);
            } else if (object instanceof Edge) {
                this.objects.add(DetachedEdge.detach((Edge) object));
                this.labels.add(as);
            } else if (object instanceof VertexProperty) {
                this.objects.add(DetachedVertexProperty.detach((VertexProperty) object));
                this.labels.add(as);
            } else if (object instanceof Property) {
                this.objects.add(DetachedProperty.detach((Property) object));
                this.labels.add(as);
            } else {
                this.objects.add(object);
                this.labels.add(as);
            }
        });
    }

    public Path attach(final Graph graph) {
        final Path path = new DefaultMutablePath();
        this.forEach((as, object) -> {
            if (object instanceof DetachedVertex) {
                path.extend(as, ((DetachedVertex) object).attach(graph));
            } else if (object instanceof DetachedEdge) {
                path.extend(as, ((DetachedEdge) object).attach(graph));
            } else if (object instanceof DetachedVertexProperty) {
                path.extend(as, ((DetachedVertexProperty) object).attach(graph));
            } else if (object instanceof DetachedProperty) {
                path.extend(as, ((DetachedProperty) object).attach(graph));
            } else {
                path.extend(as, object);
            }
        });
        return path;
    }

    public static DetachedPath detach(final Path path) {
        return new DetachedPath(path);
    }


}
