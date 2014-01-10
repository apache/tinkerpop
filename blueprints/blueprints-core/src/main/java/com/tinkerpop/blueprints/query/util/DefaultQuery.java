package com.tinkerpop.blueprints.query.util;

import com.tinkerpop.blueprints.AnnotatedList;
import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.query.Query;
import com.tinkerpop.blueprints.util.StringFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiPredicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class DefaultQuery implements Query {

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

    public Query has(final String key, final BiPredicate biPredicate, final Object value) {
        this.hasContainers.add(new HasContainer(key, biPredicate, value));
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

    ////////////////////

    protected static class HasContainer {
        public String key;
        public Object value;
        public BiPredicate predicate;

        public HasContainer(final String key, final BiPredicate predicate, final Object value) {
            this.key = key;
            this.value = value;
            this.predicate = predicate;
        }

        public boolean testElement(final Element element) {
            if (this.key.equals(Property.Key.ID))
                return this.predicate.test(element.getId(), this.value);
            else if (this.key.equals(Property.Key.LABEL))
                return this.predicate.test(element.getLabel(), this.value);
            else // TODO: Optional validation // TODO: if value is AnnotatedList && CONTAINS.IN, check list.
                return this.predicate.test(element.getValue(this.key), this.value);
        }

        public static boolean testAllOfElement(final Element element, final List<HasContainer> hasContainers) {
            return hasContainers.size() == 0 || hasContainers.stream().filter(c -> c.testElement(element)).count() == hasContainers.size();
        }

        //////////////

        public <V> boolean testAnnotatedValue(final AnnotatedList.AnnotatedValue<V> annotatedValue) {
            if (this.key.equals(StringFactory.VALUE))
                return this.predicate.test(annotatedValue.getValue(), this.value);

            if (null == annotatedValue.getAnnotations() || !annotatedValue.getAnnotations().get(this.key).isPresent())
                return false;

            return this.predicate.test(annotatedValue.getAnnotations().get(this.key).get(), this.value);
        }

        public static <V> boolean testAllOfAnnotatedValue(final AnnotatedList.AnnotatedValue<V> annotatedValue, final List<HasContainer> hasContainers) {
            return hasContainers.size() == 0 || hasContainers.stream().filter(c -> c.testAnnotatedValue(annotatedValue)).count() == hasContainers.size();
        }
    }
}
