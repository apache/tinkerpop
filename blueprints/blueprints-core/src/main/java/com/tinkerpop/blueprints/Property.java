package com.tinkerpop.blueprints;

import com.tinkerpop.blueprints.util.ElementHelper;
import com.tinkerpop.blueprints.util.StringFactory;

import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract interface Property<V> extends Annotatable {

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

    public V get() throws NoSuchElementException;

    /*public default String getString() {
        return (String) get();
    }

    public default Boolean getBoolean() {
        return (Boolean) get();
    }

    public default Integer getInt() {
        return (Integer) get();
    }

    public default Long getLong() {
        return (Long) get();
    }

    public default Float getFloat() {
        return (Float) get();
    }

    public default Double getDouble() {
        return (Double) get();
    }

    public default <T> AnnotatedList<T> getAnnotatedList() {
        return (AnnotatedList<T>) get();
    }*/

    public boolean isPresent();

    public default void ifPresent(final Consumer<? super V> consumer) {
        if (this.isPresent())
            consumer.accept(this.get());
    }

    public default V orElse(final V otherValue) {
        return this.isPresent() ? this.get() : otherValue;
    }

    public default V orElseGet(final Supplier<? extends V> supplier) {
        return this.isPresent() ? this.get() : supplier.get();
    }

    public <E extends Element> E getElement();

    public void remove();

    public static <V> Property<V> empty() {
        return new Property<V>() {
            @Override
            public String getKey() {
                throw Exceptions.propertyDoesNotExist();
            }

            @Override
            public V get() throws NoSuchElementException {
                throw Exceptions.propertyDoesNotExist();
            }

            @Override
            public boolean isPresent() {
                return false;
            }

            @Override
            public <E extends Element> E getElement() {
                throw Exceptions.propertyDoesNotExist();
            }

            @Override
            public void remove() {
                throw Exceptions.propertyDoesNotExist();
            }

            @Override
            public <V> void setAnnotation(final String key, final V value) {
                throw Exceptions.propertyDoesNotExist();
            }

            @Override
            public <V> Optional<V> getAnnotation(final String key) {
                throw Exceptions.propertyDoesNotExist();
            }

            @Override
            public String toString() {
                return StringFactory.EMPTY_PROPERTY;
            }

            @Override
            public boolean equals(final Object object) {
                return ElementHelper.areEqual(this, object);
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
