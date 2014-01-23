package com.tinkerpop.gremlin;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.micro.MicroEdge;
import com.tinkerpop.blueprints.util.micro.MicroElement;
import com.tinkerpop.blueprints.util.micro.MicroProperty;
import com.tinkerpop.blueprints.util.micro.MicroVertex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Path {

    protected List<String> asNames = new ArrayList<>();
    protected List<Object> objects = new ArrayList<>();

    public Path(final Object... asObjects) {
        if (asObjects.length % 2 != 0)
            throw new IllegalArgumentException("The provided array must be a multiple of two");
        for (int i = 0; i < asObjects.length; i = i + 2) {
            this.asNames.add((String) asObjects[i]);
            this.objects.add(asObjects[i + 1]);
        }
    }

    public int size() {
        return this.objects.size();
    }

    public void add(final String as, final Object object) {
        this.asNames.add(as);
        this.objects.add(object);
    }

    public void add(final Path path) {
        this.asNames.addAll(path.asNames);
        this.objects.addAll(path.objects);
    }

    public <T> T get(final String as) {
        for (int i = 0; i < this.asNames.size(); i++) {
            if (this.asNames.get(i).equals(as))
                return (T) this.objects.get(i);
        }
        throw new IllegalArgumentException("The as-step does not exist: " + as);
    }

    public <T> T get(final int index) {
        return (T) this.objects.get(index);
    }

    public List<String> getAsSteps() {
        return this.asNames.stream().collect(Collectors.toList());
    }

    public void renameLastStep(final String as) {
        this.asNames.set(this.asNames.size() - 1, as);
    }

    public boolean isSimple() {
        return new LinkedHashSet<>(this.objects).size() == this.objects.size();
    }

    public void forEach(final Consumer<Object> consumer) {
        this.objects.forEach(consumer);
    }

    public void forEach(final BiConsumer<String, Object> consumer) {
        for (int i = 0; i < this.size(); i++) {
            consumer.accept(this.asNames.get(i), this.objects.get(i));
        }
    }

    public void microSize() {
        final List<Object> newObjects = new ArrayList<>();
        this.objects.forEach(a -> {
            if (a instanceof MicroElement || a instanceof MicroProperty) {
                newObjects.add(a);
            } else if (a instanceof Vertex) {
                newObjects.add(new MicroVertex((Vertex) a));
            } else if (a instanceof Edge) {
                newObjects.add(new MicroEdge((Edge) a));
            } else if (a instanceof Property) {
                newObjects.add(new MicroProperty((Property) a));
            } else {
                newObjects.add(a);
            }
        });
        this.objects = newObjects;
    }

    public Path subset(final String... ases) {
        final List list = Arrays.asList(ases);
        Path path = new Path();
        this.forEach((as, object) -> {
            if (list.contains(as)) {
                path.add(as, object);
            }
        });
        return path;
    }

    public String toString() {
        return this.objects.toString();
    }
}
