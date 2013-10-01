package com.tinkerpop.blueprints;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Property<T, E extends Element> {

    public enum Key {
        ID, LABEL;

        private static final String LABEL_STRING = "label";
        private static final String ID_STRING = "id";

        public String toString() {
            if (this == ID) {
                return ID_STRING;
            } else {
                return LABEL_STRING;
            }
        }
    }

    public E getElement();

    public String getKey();

    public T getValue() throws NoSuchElementException;

    public boolean isPresent();

    public default void ifPresent(Consumer<? super T> consumer) {
        if (this.isPresent())
            consumer.accept(this.getValue());
    }

    public default T orElse(T otherValue) {
        return isPresent() ? this.getValue() : otherValue;
    }

    public default T orElseGet(Supplier<? extends T> supplier) {
        return isPresent() ? this.getValue() : supplier.get();
    }

    public default boolean is(Key reservedKey) {
        return this.getKey().equals(reservedKey.toString());
    }

    public <R> void setMetaValue(String key, R value);

    public <R> R getMetaValue(String key);

    public <R> R removeMetaValue(String key);

    public static Property[] of(Object... keyValues) {
        if (keyValues.length % 2 != 0)
            throw new IllegalArgumentException("The provided arguments must have a size that is a factor of 2");
        final Property[] properties = new Property[keyValues.length / 2];
        for (int i = 0; i < keyValues.length; i = i + 2) {
            final String key = Objects.requireNonNull(keyValues[i]).toString();
            final Object value = Objects.requireNonNull(keyValues[i + 1]);

            properties[i / 2] = new Property() {
                final Map<String, Object> metas = new HashMap<>();

                @Override
                public String getKey() {
                    return key;
                }

                @Override
                public Object getValue() throws NoSuchElementException {
                    return value;
                }

                @Override
                public Element getElement() {
                    throw new IllegalStateException("The is a container is and is not attached to an element");
                }

                @Override
                public boolean isPresent() {
                    return true;
                }

                @Override
                public void setMetaValue(String key, Object value) {
                    this.metas.put(key, value);
                }

                @Override
                public Object getMetaValue(String key) {
                    return this.metas.get(key);
                }

                @Override
                public Object removeMetaValue(String key) {
                    return this.metas.remove(key);
                }
            };
        }
        return properties;
    }

    public static <T, E extends Element> Property<T, E> empty() {
        return new Property<T, E>() {
            private static final String EMPTY_KEY = "empty";
            private static final String EMPTY_MESSAGE = "This is an empty property";

            @Override
            public String getKey() {
                return EMPTY_KEY;
            }

            @Override
            public T getValue() throws NoSuchElementException {
                throw new NoSuchElementException();
            }

            @Override
            public boolean isPresent() {
                return false;
            }

            @Override
            public E getElement() {
                throw new IllegalStateException(EMPTY_MESSAGE);
            }

            @Override
            public <R extends Object> void setMetaValue(String key, R value) {
                throw new IllegalStateException(EMPTY_MESSAGE);
            }

            @Override
            public <R extends Object> R getMetaValue(String key) {
                throw new IllegalStateException(EMPTY_MESSAGE);
            }

            @Override
            public <R extends Object> R removeMetaValue(String key) {
                throw new IllegalStateException(EMPTY_MESSAGE);
            }
        };
    }
}
