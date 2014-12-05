package com.tinkerpop.gremlin.structure.util.detached;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.util.MutablePath;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DetachedPath extends MutablePath {

    public DetachedPath() {

    }

    public DetachedPath(final Path path, final boolean asReference) {
        path.forEach((labels, object) -> {
            if (object instanceof DetachedElement || object instanceof DetachedProperty || object instanceof DetachedPath) {
                this.labels.add(labels);
                this.objects.add(object);
            } else if (object instanceof Element) {
                this.labels.add(labels);
                this.objects.add(DetachedFactory.detach((Element) object, asReference));
            } else if (object instanceof Property) {
                this.labels.add(labels);
                this.objects.add(DetachedFactory.detach((Property) object));
            } else if (object instanceof Path) {
                this.labels.add(labels);
                this.objects.add(DetachedFactory.detach((Path) object, asReference));
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
