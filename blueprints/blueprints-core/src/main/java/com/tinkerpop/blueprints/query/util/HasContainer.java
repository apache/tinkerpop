package com.tinkerpop.blueprints.query.util;

import com.tinkerpop.blueprints.AnnotatedValue;
import com.tinkerpop.blueprints.Annotations;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Property;

import java.util.List;
import java.util.function.BiPredicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HasContainer {

    public String key;
    public BiPredicate predicate;
    public Object value;

    public HasContainer(final String key, final BiPredicate predicate, final Object value) {
        this.key = key;
        this.predicate = predicate;
        this.value = value;
    }

    public boolean test(final Element element) {
        if (this.key.equals(Property.Key.ID))
            return this.predicate.test(element.getId(), this.value);
        else if (this.key.equals(Property.Key.LABEL))
            return this.predicate.test(element.getLabel(), this.value);
        else {
            final Property property = element.getProperty(this.key);
            return property.isPresent() && this.predicate.test(property.get(), this.value);
        }
    }

    public static boolean testAll(final Element element, final List<HasContainer> hasContainers) {
        if (hasContainers.size() == 0)
            return true;
        else {
            for (final HasContainer hasContainer : hasContainers) {
                if (!hasContainer.test(element))
                    return false;
            }
            return true;
        }
    }

    public <V> boolean test(final AnnotatedValue<V> annotatedValue) {
        if (this.key.equals(Annotations.Key.VALUE))
            return this.predicate.test(annotatedValue.getValue(), this.value);

        if (null == annotatedValue.getAnnotations() || !annotatedValue.getAnnotations().get(this.key).isPresent())
            return false;

        return this.predicate.test(annotatedValue.getAnnotations().get(this.key).get(), this.value);
    }

    public static <V> boolean testAll(final AnnotatedValue<V> annotatedValue, final List<HasContainer> hasContainers) {
        if (hasContainers.size() == 0)
            return true;
        else {
            for (final HasContainer hasContainer : hasContainers) {
                if (!hasContainer.test(annotatedValue))
                    return false;
            }
            return true;
        }
    }

}