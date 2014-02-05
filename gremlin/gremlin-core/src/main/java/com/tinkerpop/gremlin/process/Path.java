package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.structure.util.ObjectHelper;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Path {

    protected List<String> asLabels = new ArrayList<>();
    protected List<Object> objects = new ArrayList<>();

    public Path(final Object... asObjects) {
        if (asObjects.length % 2 != 0)
            throw new IllegalArgumentException("The provided array must be a multiple of two");
        for (int i = 0; i < asObjects.length; i = i + 2) {
            this.asLabels.add((String) asObjects[i]);
            this.objects.add(asObjects[i + 1]);
        }
    }

    public int size() {
        return this.objects.size();
    }

    public void add(final String as, final Object object) {
        this.asLabels.add(as);
        this.objects.add(object);
    }

    public void add(final Path path) {
        this.asLabels.addAll(path.asLabels);
        this.objects.addAll(path.objects);
    }

    public <T> T get(final String as) {
        for (int i = 0; i < this.asLabels.size(); i++) {
            if (this.asLabels.get(i).equals(as))
                return (T) this.objects.get(i);
        }
        throw new IllegalArgumentException("The as-step does not exist: " + as);
    }

    public <T> T get(final int index) {
        return (T) this.objects.get(index);
    }

    public List<String> getAsLabels() {
        return this.asLabels.stream().collect(Collectors.toList());
    }

    public void renameLastStep(final String as) {
        this.asLabels.set(this.asLabels.size() - 1, as);
    }

    public boolean isSimple() {
        return new LinkedHashSet<>(this.objects).size() == this.objects.size();
    }

    public void forEach(final Consumer<Object> consumer) {
        this.objects.forEach(consumer);
    }

    public void forEach(final BiConsumer<String, Object> consumer) {
        for (int i = 0; i < this.size(); i++) {
            consumer.accept(this.asLabels.get(i), this.objects.get(i));
        }
    }

    public Path subset(final String... asLabels) {
        final Path path = new Path();
        this.forEach((as, object) -> {
            if (ObjectHelper.contains(as, asLabels)) {
                path.add(as, object);
            }
        });
        return path;
    }

    public String toString() {
        return this.objects.toString();
    }
}
