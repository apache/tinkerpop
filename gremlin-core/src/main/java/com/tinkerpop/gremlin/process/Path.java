package com.tinkerpop.gremlin.process;

import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * A Path denotes a particular walk through a {@link com.tinkerpop.gremlin.structure.Graph} as defined by a {@link Traverser}.
 * Internal to a Path are two lists: a list of labels and a list of objects.
 * The list of labels are the as-labels of the steps traversed.
 * The list of objects are the objects traversed.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Path {

    protected List<String> asLabels = new ArrayList<>();
    protected List<Object> objects = new ArrayList<>();

    protected Path() {

    }

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

    public boolean hasAs(final String as) {
        return this.asLabels.contains(as);
    }

    // TODO: why does this have to exist. I hate this.
    public void renameLastStep(final String as) {
        this.asLabels.set(this.asLabels.size() - 1, as);
    }

    /**
     * Determines whether the path is a simple or not.
     * A simple path has no cycles and thus, no repeated objects.
     *
     * @return Whether the path is simple or not
     */
    public boolean isSimple() {
        return new HashSet<>(this.objects).size() == this.objects.size();
    }

    public void forEach(final Consumer<Object> consumer) {
        this.objects.forEach(consumer);
    }

    public void forEach(final BiConsumer<String, Object> consumer) {
        for (int i = 0; i < this.size(); i++) {
            consumer.accept(this.asLabels.get(i), this.objects.get(i));
        }
    }

    public Stream<Pair<String, Object>> stream() {
        return IntStream.range(0, this.size()).mapToObj(i -> Pair.with(this.asLabels.get(i), this.objects.get(i)));
    }

    public String toString() {
        return this.objects.toString();
    }
}
