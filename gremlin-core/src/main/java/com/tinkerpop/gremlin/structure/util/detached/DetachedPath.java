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
public class DetachedPath extends MutablePath implements Attachable<Path> {

    public DetachedPath() {

    }

    protected DetachedPath(final Path path, final boolean withProperties) {
        path.forEach((object,labels) -> {
            if (object instanceof DetachedElement || object instanceof DetachedProperty || object instanceof DetachedPath) {
                this.labels.add(labels);
                this.objects.add(object);
            } else if (object instanceof Element) {
                this.labels.add(labels);
                this.objects.add(DetachedFactory.detach((Element) object, withProperties));
            } else if (object instanceof Property) {
                this.labels.add(labels);
                this.objects.add(DetachedFactory.detach((Property) object));
            } else if (object instanceof Path) {
                this.labels.add(labels);
                this.objects.add(DetachedFactory.detach((Path) object, withProperties));
            } else {
                this.labels.add(labels);
                this.objects.add(object);
            }
        });
    }

    @Override
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
        this.forEach((object,labels) -> {
            if (object instanceof Attachable) {
                path.extend(((Attachable) object).attach(hostGraph), labels.toArray(new String[labels.size()]));
            } else {
                path.extend(object, labels.toArray(new String[labels.size()]));
            }
        });
        return path;
    }

    @Override
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
        this.forEach((object,labels) -> {
            if (object instanceof Attachable) {
                path.extend(((Attachable) object).attach(hostVertex), labels.toArray(new String[labels.size()]));
            } else {
                path.extend(object, labels.toArray(new String[labels.size()]));
            }
        });
        return path;
    }

    public String toString() {
        return this.objects.toString();
    }
}
