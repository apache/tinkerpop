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

    protected List<Set<String>> labels = new ArrayList<>();
    protected List<Object> objects = new ArrayList<>();

    public Path() {

    }

    public int size() {
        return this.objects.size();
    }

    public void add(final String label, final Object object) {
        final Set<String> labels = new HashSet<>();
        if (TraversalHelper.isLabeled(label))
            labels.add(label);
        this.labels.add(labels);
        this.objects.add(object);
    }

    public void add(final Set<String> labels, final Object object) {
        this.labels.add(labels.stream().filter(TraversalHelper::isLabeled).collect(Collectors.toSet()));
        this.objects.add(object);
    }

    public void add(final Path path) {
        this.labels.addAll(path.labels);
        this.objects.addAll(path.objects);
    }

    public <A> A get(final String label) {
        for (int i = 0; i < this.labels.size(); i++) {
            if (this.labels.get(i).contains(label))
                return (A) this.objects.get(i);
        }
        throw new IllegalArgumentException("The step with label " + label + "  does not exist");
    }

    public <A> A get(final int index) {
        return (A) this.objects.get(index);
    }

    public boolean hasLabel(final String label) {
        for (final Set<String> labels : this.labels) {
            if (labels.contains(label))
                return true;
        }
        return false;
    }

    public void addLabel(final String label) {
        if (TraversalHelper.isLabeled(label))
            this.labels.get(this.labels.size() - 1).add(label);
    }

    public List<Object> getObjects() {
        return new ArrayList<>(this.objects);
    }

    public List<Set<String>> getLabels() {
        final List<Set<String>> labelSets = new ArrayList<>();
        for (final Set<String> set : this.labels) {
            labelSets.add(new HashSet<>(set));
        }
        return labelSets;
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
            consumer.accept(this.labels.get(i), this.objects.get(i));
        }
    }

    public Stream<Pair<Set<String>, Object>> stream() {
        return IntStream.range(0, this.size()).mapToObj(i -> Pair.with(this.labels.get(i), this.objects.get(i)));
    }

    public String toString() {
        return this.objects.toString();
    }

}
