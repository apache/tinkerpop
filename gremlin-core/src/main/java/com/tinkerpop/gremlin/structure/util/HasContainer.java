package com.tinkerpop.gremlin.structure.util;

import com.tinkerpop.gremlin.structure.AnnotatedValue;
import com.tinkerpop.gremlin.structure.Contains;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;

import java.io.Serializable;
import java.util.List;
import java.util.function.BiPredicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HasContainer implements Serializable {

    public String key;
    public BiPredicate predicate;
    public Object value;

    public HasContainer(final String key, final BiPredicate predicate, final Object value) {
        this.key = key;
        this.predicate = predicate;
        this.value = value;
        if (null == this.value && !(this.predicate instanceof Contains)) {
            throw new IllegalArgumentException("For determining the existence of a property, use the Contains predicate");
        }
    }

    public HasContainer(final String key, final Contains contains) {
        this(key, contains, null);
    }

    public boolean test(final Element element) {
        if (null != this.value) {
            if (this.key.equals(Element.ID))
                return this.predicate.test(element.id(), this.value);
            else if (this.key.equals(Element.LABEL))
                return this.predicate.test(element.label(), this.value);
            else {
                final Property property = element.property(this.key);
                return property.isPresent() && this.predicate.test(property.value(), this.value);
            }
        } else {
            return Contains.IN.equals(this.predicate) ?
                    element.property(this.key).isPresent() :
                    !element.property(this.key).isPresent();
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
        if (null != this.value) {
            if (this.key.equals(AnnotatedValue.VALUE))
                return this.predicate.test(annotatedValue.getValue(), this.value);

            if (!annotatedValue.getAnnotation(this.key).isPresent())
                return false;

            return this.predicate.test(annotatedValue.getAnnotation(this.key).get(), this.value);
        } else {
            return Contains.IN.equals(this.predicate) ?
                    annotatedValue.getAnnotation(this.key).isPresent() :
                    !annotatedValue.getAnnotation(this.key).isPresent();
        }
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

    public String toString() {
        return this.value == null ?
                (this.predicate == Contains.IN ?
                        "[" + this.key + "]" :
                        "[!" + this.key + "]") :
                "[" + this.key + "," + this.predicate + "," + this.value + "]";
    }

}