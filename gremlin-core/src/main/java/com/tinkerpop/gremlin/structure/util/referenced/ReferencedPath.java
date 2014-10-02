package com.tinkerpop.gremlin.structure.util.referenced;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.util.DefaultMutablePath;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.detached.Attachable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ReferencedPath extends DefaultMutablePath implements Attachable, Serializable {

    public ReferencedPath() {

    }

    public ReferencedPath(final Path path) {
        path.forEach((labels, object) -> {
            if (object instanceof ReferencedElement || object instanceof ReferencedProperty || object instanceof ReferencedPath) {
                this.labels.add(labels);
                this.objects.add(object);
            } else if (object instanceof Element) {
                this.labels.add(labels);
                this.objects.add(ReferencedFactory.detach((Element) object));
            } else if (object instanceof Property) {
                this.labels.add(labels);
                this.objects.add(ReferencedFactory.detach((Property) object));
            } else if (object instanceof Path) {
                this.labels.add(labels);
                this.objects.add(ReferencedFactory.detach((Path) object));
            } else {
                this.labels.add(labels);
                this.objects.add(object);
            }
        });
    }

    public Path attach(final Graph hostGraph) {
        final Path path = new DefaultMutablePath();
        this.forEach((labels, object) -> {
            if (object instanceof Attachable) {
                path.extend(labels, ((Attachable) object).attach(hostGraph));
            } else {
                path.extend(labels, object);
            }
        });
        return path;
    }

    public Path attach(final Vertex hostVertex) {
        final Path path = new DefaultMutablePath();
        this.forEach((labels, object) -> {
            if (object instanceof Attachable) {
                path.extend(labels, ((Attachable) object).attach(hostVertex));
            } else {
                path.extend(labels, object);
            }
        });
        return path;
    }
}
