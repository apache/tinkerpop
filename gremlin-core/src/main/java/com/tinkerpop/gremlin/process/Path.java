package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.process.util.TraversalHelper;
import org.javatuples.Pair;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
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
public class Path implements Serializable {

    protected List<Set<String>> asLabels = new ArrayList<>();
    protected List<Object> objects = new ArrayList<>();

    public Path() {

    }

    public int size() {
        return this.objects.size();
    }

    public void add(final String as, final Object object) {
        final Set<String> labels = new HashSet<>();
        if (TraversalHelper.isLabeled(as))
            labels.add(as);
        this.asLabels.add(labels);
        this.objects.add(object);
    }

    public void add(final Set<String> asLabels, final Object object) {
        this.asLabels.add(asLabels.stream().filter(TraversalHelper::isLabeled).collect(Collectors.toSet()));
        this.objects.add(object);
    }

    public void add(final Path path) {
        this.asLabels.addAll(path.asLabels);
        this.objects.addAll(path.objects);
    }

    public <A> A get(final String as) {
        for (int i = 0; i < this.asLabels.size(); i++) {
            if (this.asLabels.get(i).contains(as))
                return (A) this.objects.get(i);
        }
        throw new IllegalArgumentException("The as-step does not exist: " + as);
    }

    public <A> A get(final int index) {
        return (A) this.objects.get(index);
    }

    public boolean hasAs(final String as) {
        for (final Set<String> labels : this.asLabels) {
            if (labels.contains(as))
                return true;
        }
        return false;
    }

    public void addAs(final String as) {
        if (TraversalHelper.isLabeled(as))
            this.asLabels.get(this.asLabels.size() - 1).add(as);
    }

    public List<Object> getObjects() {
        return new ArrayList<>(this.objects);
    }

    public List<Set<String>> getAsLabels() {
        final List<Set<String>> labels = new ArrayList<>();
        for (final Set<String> set : this.asLabels) {
            labels.add(new HashSet<>(set));
        }
        return labels;
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

    public void forEach(final BiConsumer<Set<String>, Object> consumer) {
        for (int i = 0; i < this.size(); i++) {
            consumer.accept(this.asLabels.get(i), this.objects.get(i));
        }
    }

    public Stream<Pair<Set<String>, Object>> stream() {
        return IntStream.range(0, this.size()).mapToObj(i -> Pair.with(this.asLabels.get(i), this.objects.get(i)));
    }

    public String toString() {
        return this.objects.toString();
    }

}
