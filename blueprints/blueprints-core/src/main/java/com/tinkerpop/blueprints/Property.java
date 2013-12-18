package com.tinkerpop.blueprints;

import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract interface Property<V> {

    public class Key {

        public static final String ID = "id";
        public static final String LABEL = "label";
        public static final String DEFAULT_LABEL = "default";

        private static final String HIDDEN_PREFIX = "%&%";

        public static String hidden(final String key) {
            return HIDDEN_PREFIX.concat(key);
        }
    }

    public String getKey();

    public V getValue() throws NoSuchElementException;

    public boolean isPresent();

    public default void ifPresent(Consumer<? super V> consumer) {
        if (this.isPresent())
            consumer.accept(this.getValue());
    }

    public default V orElse(V otherValue) {
        return this.isPresent() ? this.getValue() : otherValue;
    }

    public default V orElseGet(Supplier<? extends V> supplier) {
        return this.isPresent() ? this.getValue() : supplier.get();
    }

    public void remove();

    public static <V> Property<V> empty() {
        return new Property<V>() {
            @Override
            public String getKey() {
                throw Exceptions.propertyDoesNotExist();
            }

            @Override
            public V getValue() throws NoSuchElementException {
                throw Exceptions.propertyDoesNotExist();
            }

            @Override
            public boolean isPresent() {
                return false;
            }

            @Override
            public void remove() {
                throw Exceptions.propertyDoesNotExist();
            }
        };
    }

    public static class Exceptions {
        public static IllegalArgumentException propertyKeyIsReserved(final String key) {
            return new IllegalArgumentException("Property key is reserved for all elements: " + key);
        }

        public static IllegalArgumentException propertyKeyIdIsReserved() {
            return propertyKeyIsReserved(Property.Key.ID);
        }

        public static IllegalArgumentException propertyKeyLabelIsReserved() {
            return propertyKeyIsReserved(Property.Key.LABEL);
        }

        public static IllegalArgumentException propertyKeyCanNotBeEmpty() {
            return new IllegalArgumentException("Property key can not be the empty string");
        }

        public static IllegalArgumentException propertyKeyCanNotBeNull() {
            return new IllegalArgumentException("Property key can not be null");
        }

        public static IllegalArgumentException propertyValueCanNotBeNull() {
            return new IllegalArgumentException("Property value can not be null");
        }

        public static IllegalStateException propertyDoesNotExist() {
            throw new IllegalStateException("The property does not exist as it has no key, value, or associated element");
        }

        public static UnsupportedOperationException dataTypeOfPropertyValueNotSupported(final Object val) {
            throw new UnsupportedOperationException(String.format("Property value [%s] is of type %s which is not supported by this Graph implementation", val, val.getClass()));
        }
    }


}
