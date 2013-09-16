package com.tinkerpop.blueprints.util;

import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Query;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class DefaultQuery implements Query {

    private static final String[] EMPTY_LABELS = new String[]{};

    public Direction direction = Direction.BOTH;
    public String[] labels = EMPTY_LABELS;
    public int limit = Integer.MAX_VALUE;
    public List<HasContainer> hasContainers = new ArrayList<>();

    public Query has(final String key, final Object value) {
        this.hasContainers.add(new HasContainer(key, Compare.EQUAL, value));
        return this;
    }

    public Query hasNot(final String key, final Object value) {
        this.hasContainers.add(new HasContainer(key, Compare.NOT_EQUAL, value));
        return this;
    }

    public Query hasNot(final String key) {
        this.hasContainers.add(new HasContainer(key, Compare.EQUAL, null));
        return this;
    }

    public Query has(final String key) {
        this.hasContainers.add(new HasContainer(key, Compare.NOT_EQUAL, null));
        return this;
    }

    public Query has(final String key, final BiPredicate predicate, final Object value) {
        this.hasContainers.add(new HasContainer(key, predicate, value));
        return this;
    }

    public <T extends Comparable<?>> Query interval(final String key, final T startValue, final T endValue) {
        this.hasContainers.add(new HasContainer(key, Compare.GREATER_THAN_EQUAL, startValue));
        this.hasContainers.add(new HasContainer(key, Compare.LESS_THAN, endValue));
        return this;
    }

    public Query limit(final int limit) {
        this.limit = limit;
        return this;
    }

    public boolean test(final Element element) {
        return this.hasContainers.stream().filter(c -> c.test(element)).iterator().hasNext();
    }

    ////////////////////


    protected static class HasContainer implements Predicate<Element> {
        public String key;
        public Object value;
        public BiPredicate predicate;

        public HasContainer(final String key, final BiPredicate predicate, final Object value) {
            this.key = key;
            this.value = value;
            this.predicate = predicate;
        }

        public boolean test(final Element element) {
            if (this.key.equals(Property.Key.ID.toString()))
                return element.getId().equals(this.value);
            else if (element instanceof Edge && this.key.equals(Property.Key.LABEL.toString()))
                return ((Edge) element).getLabel().equals(this.value);
            else
                return this.predicate.test(element.getValue(this.key), this.value);
        }

        public static boolean testAll(final Element element, final List<HasContainer> hasContainers) {
            return hasContainers.size() == 0 || hasContainers.stream().filter(c -> c.test(element)).count() == hasContainers.size();
        }
    }
}
