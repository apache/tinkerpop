package com.tinkerpop.blueprints;

import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Annotation<V> {

    public String getKey();

    public V getValue() throws NoSuchElementException;

    public boolean isPresent();

    public default void ifPresent(final Consumer<? super V> consumer) {
        if (this.isPresent())
            consumer.accept(this.getValue());
    }

    public default V orElse(final V otherValue) {
        return this.isPresent() ? this.getValue() : otherValue;
    }

    public default V orElseGet(final Supplier<? extends V> supplier) {
        return this.isPresent() ? this.getValue() : supplier.get();
    }

    public void remove();

    public static <V> Annotation<V> empty() {
        return new Annotation<V>() {
            @Override
            public String getKey() {
                throw Exceptions.annotationDoesNotExist();
            }

            @Override
            public V getValue() throws NoSuchElementException {
                throw Exceptions.annotationDoesNotExist();
            }

            @Override
            public boolean isPresent() {
                return false;
            }

            @Override
            public void remove() {
                throw Exceptions.annotationDoesNotExist();
            }
        };
    }

    public static class Exceptions {

        public static IllegalArgumentException annotationKeyCanNotBeEmpty() {
            return new IllegalArgumentException("Annotation key can not be the empty string");
        }

        public static IllegalArgumentException annotationKeyCanNotBeNull() {
            return new IllegalArgumentException("Annotation key can not be null");
        }

        public static IllegalArgumentException annotationValueCanNotBeNull() {
            return new IllegalArgumentException("Annotation value can not be null");
        }

        public static IllegalStateException annotationDoesNotExist() {
            throw new IllegalStateException("The annotation does not exist as it has no key or value");
        }

        public static UnsupportedOperationException dataTypeOfAnnotationValueNotSupported(final Object val) {
            throw new UnsupportedOperationException(String.format("Annotation value [%s] is of type %s which is not supported by this Graph implementation", val, val.getClass()));
        }
    }
}
