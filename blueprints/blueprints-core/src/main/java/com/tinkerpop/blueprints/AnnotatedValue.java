package com.tinkerpop.blueprints;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface AnnotatedValue<V> {

    public V getValue();

    public Annotations getAnnotations();

    public void remove();

    public static class Exceptions {

        public static IllegalArgumentException annotatedValueCanNotBeNull() {
            return new IllegalArgumentException("The annotated value can not be null");
        }
    }
}
