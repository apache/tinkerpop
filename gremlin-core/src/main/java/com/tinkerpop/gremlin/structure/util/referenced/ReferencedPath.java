package com.tinkerpop.gremlin.structure.util.referenced;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.util.MutablePath;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.detached.Attachable;

import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ReferencedPath extends MutablePath implements Attachable, Serializable {

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
        /*Path path = EmptyPath.instance();
        for (int i = 0; i < this.objects.size(); i++) {
            if (this.objects.get(i) instanceof Attachable) {
                path.extend(this.labels.get(i), ((Attachable) this.objects.get(i)).attach(hostGraph));
            } else {
                path.extend(this.labels.get(i), this.objects.get(i));
            }

            path = path.extend(this.labels.get(i), this.objects.get(i));
        }
        return path;*/

        final Path path = MutablePath.make();
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
        /*Path path = EmptyPath.instance();
        for (int i = 0; i < this.objects.size(); i++) {
            if (this.objects.get(i) instanceof Attachable) {
                path.extend(this.labels.get(i), ((Attachable) this.objects.get(i)).attach(hostVertex));
            } else {
                path.extend(this.labels.get(i), this.objects.get(i));
            }

            path = path.extend(this.labels.get(i), this.objects.get(i));
        }
        return path;*/
        final Path path = MutablePath.make();
        this.forEach((labels, object) -> {
            if (object instanceof Attachable) {
                path.extend(labels, ((Attachable) object).attach(hostVertex));
            } else {
                path.extend(labels, object);
            }
        });
        return path;
    }

    public String toString() {
        return this.objects.toString();
    }
}
