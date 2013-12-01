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

    public default boolean is(Key reservedKey) {
        return this.getKey().equals(reservedKey.toString());
    }

    public void remove();

    public static Property.Features getFeatures() {
        return new Features() {
        };
    }

    public interface Features extends com.tinkerpop.blueprints.Features {

        public default boolean supportsMetaProperties() {
            return true;
        }

        public default boolean supportsStringValues() {
            return true;
        }

        public default boolean supportsIntegerValues() {
            return true;
        }

        public static IllegalArgumentException propertyKeyIsReserved(final String key) {
            return new IllegalArgumentException("Property key is reserved for all elements: " + key);
        }

        public static IllegalArgumentException propertyKeyIdIsReserved() {
            return new IllegalArgumentException("Property key is reserved for all elements: id");
        }

        public static IllegalArgumentException propertyKeyLabelIsReservedForEdges() {
            return new IllegalArgumentException("Property key is reserved for all edges: label");
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

        public static IllegalStateException propertyPropertyCanNotHaveAProperty() {
            throw new IllegalStateException("A property's property can not have a property");
        }

        public static IllegalStateException propertyDoesNotExist() {
            throw new IllegalStateException("The property does not exist as it has no key, value, or associated element");
        }
    }

    public static <V> Property<V> empty() {
        return new Property<V>() {
            @Override
            public String getKey() {
                throw Features.propertyDoesNotExist();
            }

            @Override
            public V getValue() throws NoSuchElementException {
                throw Features.propertyDoesNotExist();
            }

            @Override
            public boolean isPresent() {
                return false;
            }

            @Override
            public void remove() {
                throw Features.propertyDoesNotExist();
            }
        };
    }
}
