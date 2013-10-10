package com.tinkerpop.gremlin.pipes.util;

import com.tinkerpop.gremlin.pipes.Pipe;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Path {

    protected ArrayList<String> names = new ArrayList<>();
    protected ArrayList<Object> objects = new ArrayList<>();

    public Path() {
        super();
    }

    public Path(final Path path) {
        this.add(path);
    }

    public int size() {
        return this.objects.size();
    }

    public void add(final String name, final Object object) {
        this.names.add(name);
        this.objects.add(object);
    }

    public void add(final Path path) {
        this.names.addAll(path.names);
        this.objects.addAll(path.objects);
    }

    public <T> T get(final String name) {
        for (int i = 0; i < this.names.size(); i++) {
            if (this.names.get(i).equals(name))
                return (T) this.objects.get(i);
        }
        throw new IllegalArgumentException("The named step does not exist: " + name);
    }

    public List<String> getNamedSteps() {
        return this.names.stream().filter(s -> !s.equals(Pipe.NONE)).collect(Collectors.toList());
    }

    public boolean isSimple() {
        return new LinkedHashSet(this.objects).size() == this.objects.size();
    }

    public String toString() {
        return this.objects.toString();
    }
}
