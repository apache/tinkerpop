package com.tinkerpop.blueprints;

import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Property<T> {

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
        throw new UnsupportedOperationException();
    }

    public static <T> Property<T> empty() {
        return new Property<T>() {
            private static final String EMPTY_MESSAGE = "This is an empty property";

            @Override
            public String getKey() {
                return "empty";
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
