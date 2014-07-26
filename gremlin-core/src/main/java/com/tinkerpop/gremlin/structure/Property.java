package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract interface Property<V> {

    public String key();

    public V value() throws NoSuchElementException;

    public boolean isPresent();

    public default void ifPresent(final Consumer<? super V> consumer) {
        if (this.isPresent())
            consumer.accept(this.value());
    }

    public default V orElse(final V otherValue) {
        return this.isPresent() ? this.value() : otherValue;
    }

    public default V orElseGet(final Supplier<? extends V> edgeSupplier) {
        return this.isPresent() ? this.value() : edgeSupplier.get();
    }

    public default <E extends Throwable> V orElseThrow(final Supplier<? extends E> exceptionSupplier) throws E {
        if (this.isPresent()) return this.value();
        else
            throw exceptionSupplier.get();
    }

    public boolean isHidden();

    public <E extends Element> E getElement();

    public void remove();

    public static <V> Property<V> empty() {
        return new Property<V>() {
            @Override
            public String key() {
                throw Exceptions.propertyDoesNotExist();
            }

            @Override
            public V value() throws NoSuchElementException {
                throw Exceptions.propertyDoesNotExist();
            }

            @Override
            public boolean isPresent() {
                return false;
            }

            @Override
            public boolean isHidden() {
                throw Exceptions.propertyDoesNotExist();
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
            public String toString() {
                return StringFactory.propertyString(this);
            }

            @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
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
            return propertyKeyIsReserved(Element.ID);
        }

        public static IllegalArgumentException propertyKeyLabelIsReserved() {
            return propertyKeyIsReserved(Element.LABEL);
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
            return new IllegalStateException("The property does not exist as it has no key, value, or associated element");
        }

        public static IllegalStateException propertyDoesNotExist(final String key) {
            return Graph.Key.isHidden(key) ? new IllegalStateException("The hidden property does not exist as the key has no associated value: " + Graph.Key.unHide(key)) :
                    new IllegalStateException("The property does not exist as the key has no associated value: " + key);
        }

        public static UnsupportedOperationException dataTypeOfPropertyValueNotSupported(final Object val) {
            return new UnsupportedOperationException(String.format("Property value [%s] is of type %s is not supported", val, val.getClass()));
        }
    }


}
