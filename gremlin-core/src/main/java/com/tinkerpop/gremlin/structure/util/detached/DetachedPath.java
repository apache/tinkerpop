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

    private DetachedPath() {

    }

    protected DetachedPath(final Path path, final boolean withProperties) {
        path.forEach((object, labels) -> {
            if (object instanceof DetachedElement || object instanceof DetachedProperty || object instanceof DetachedPath) {
                this.objects.add(object);
                this.labels.add(labels);
            } else if (object instanceof Element) {
                this.objects.add(DetachedFactory.detach((Element) object, withProperties));
                this.labels.add(labels);
            } else if (object instanceof Property) {
                this.objects.add(DetachedFactory.detach((Property) object));
                this.labels.add(labels);
            } else if (object instanceof Path) {
                this.objects.add(DetachedFactory.detach((Path) object, withProperties));
                this.labels.add(labels);
            } else {
                this.objects.add(object);
                this.labels.add(labels);
            }
        });
    }

    @Override
    public Path attach(final Graph hostGraph) {
        final Path path = MutablePath.make();  // TODO: Use ImmutablePath?
        this.forEach((object, labels) -> path.extend(object instanceof Attachable ? ((Attachable) object).attach(hostGraph) : object, labels.toArray(new String[labels.size()])));
        return path;
    }

    @Override
    public Path attach(final Vertex hostVertex) {
        final Path path = MutablePath.make();  // TODO: Use ImmutablePath?
        this.forEach((object, labels) -> path.extend(object instanceof Attachable ? ((Attachable) object).attach(hostVertex) : object, labels.toArray(new String[labels.size()])));
        return path;
    }

    public String toString() {
        return this.objects.toString();
    }
}
