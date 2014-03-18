package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.micro.MicroEdge;
import com.tinkerpop.gremlin.structure.util.micro.MicroElement;
import com.tinkerpop.gremlin.structure.util.micro.MicroProperty;
import com.tinkerpop.gremlin.structure.util.micro.MicroVertex;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MicroPath extends Path {

    private MicroPath(final Path path) {
        path.forEach((as, object) -> {
            if (object instanceof MicroElement || object instanceof MicroProperty) {
                this.add(as, object);
            } else if (object instanceof Vertex) {
                this.add(as, MicroVertex.deflate((Vertex) object));
            } else if (object instanceof Edge) {
                this.add(as, MicroEdge.deflate((Edge) object));
            } else if (object instanceof Property) {
                this.add(as, MicroProperty.deflate((Property) object));
            } else {
                this.add(as, object);
            }
        });
    }

    public Path inflate(final Graph graph) {
        final Path path = new Path();
        this.forEach((as, object) -> {
            if (object instanceof MicroVertex) {
                path.add(as, ((MicroVertex) object).inflate(graph));
            } else if (object instanceof MicroEdge) {
                path.add(as, ((MicroEdge) object).inflate(graph));
            } else if (object instanceof MicroProperty) {
                path.add(as, ((MicroProperty) object).inflate(graph));
            } else {
                path.add(as, object);
            }
        });
        return path;
    }

    public static MicroPath deflate(final Path path) {
        return new MicroPath(path);
    }
}
