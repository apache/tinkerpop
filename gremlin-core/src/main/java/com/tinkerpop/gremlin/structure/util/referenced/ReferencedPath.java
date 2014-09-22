package com.tinkerpop.gremlin.structure.util.referenced;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.detached.Attachable;

import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ReferencedPath extends Path implements Attachable, Serializable {

    public ReferencedPath() {

    }

    public ReferencedPath(final Path path) {
        path.forEach((as, object) -> {
            if (object instanceof ReferencedElement || object instanceof ReferencedProperty || object instanceof ReferencedPath) {
                this.add(as, object);
            } else if (object instanceof Element) {
                this.add(as, ReferencedFactory.detach((Element) object));
            } else if (object instanceof Property) {
                this.add(as, ReferencedFactory.detach((Property) object));
            } else if (object instanceof Path) {
                this.add(as, ReferencedFactory.detach((Path) object));
            } else {
                this.add(as, object);
            }
        });
    }

    public Path attach(final Graph hostGraph) {
        final Path path = new Path();
        this.forEach((as, object) -> {
            if (object instanceof Attachable) {
                path.add(as, ((Attachable) object).attach(hostGraph));
            } else {
                path.add(as, object);
            }
        });
        return path;
    }

    public Path attach(final Vertex hostVertex) {
        final Path path = new Path();
        this.forEach((as, object) -> {
            if (object instanceof Attachable) {
                path.add(as, ((Attachable) object).attach(hostVertex));
            } else {
                path.add(as, object);
            }
        });
        return path;
    }
}
