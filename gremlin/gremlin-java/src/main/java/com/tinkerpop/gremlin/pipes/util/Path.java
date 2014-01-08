package com.tinkerpop.gremlin.pipes.util;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Path {

    protected ArrayList<String> asNames = new ArrayList<>();
    protected ArrayList<Object> objects = new ArrayList<>();

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

    public List<String> getAsSteps() {
        return this.asNames.stream().collect(Collectors.toList());
    }

    public boolean isSimple() {
        return new LinkedHashSet<>(this.objects).size() == this.objects.size();
    }

    public void forEach(final BiConsumer<String, Object> consumer) {
        for (int i = 0; i < this.size(); i++) {
            consumer.accept(this.asNames.get(i), this.objects.get(i));
        }
    }

    public String toString() {
        return this.objects.toString();
    }
}
