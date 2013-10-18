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
public interface Property<T, E extends Thing> extends Thing {

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

    public E getThing();

    public String getKey();

    public T getValue() throws NoSuchElementException;

    public boolean isPresent();

    public default void ifPresent(Consumer<? super T> consumer) {
        if (this.isPresent())
            consumer.accept(this.getValue());
    }

    public default T orElse(T otherValue) {
        return this.isPresent() ? this.getValue() : otherValue;
    }

    public default T orElseGet(Supplier<? extends T> supplier) {
        return this.isPresent() ? this.getValue() : supplier.get();
    }

    public default boolean is(Key reservedKey) {
        return this.getKey().equals(reservedKey.toString());
    }

    public <R> Property<R, Property> setProperty(String key, R value) throws IllegalStateException;

    public <R> Property<R, Property> getProperty(String key) throws IllegalStateException;

    public <R> Property<R, Property> removeProperty(String key) throws IllegalStateException;

    public static Property[] of(Object... keyValues) {
        if (keyValues.length % 2 != 0)
            throw new IllegalArgumentException("The provided arguments must have a size that is a factor of 2");
        final Property[] properties = new Property[keyValues.length / 2];
        for (int i = 0; i < keyValues.length; i = i + 2) {
            final String key = Objects.requireNonNull(keyValues[i]).toString();
            final Object value = Objects.requireNonNull(keyValues[i + 1]);

            properties[i / 2] = new Property() {
                final Map<String, Property> metas = new HashMap<>();
                final Property p = this;

                @Override
                public String getKey() {
                    return key;
                }

                @Override
                public Object getValue() throws NoSuchElementException {
                    return value;
                }

                @Override
                public Thing getThing() {
                    throw new IllegalStateException("The is a container and is not attached to anything");
                }

                @Override
                public boolean isPresent() {
                    return true;
                }

                public Property setProperty(String aKey, Object aValue) throws IllegalStateException {
                    final Property property = this.metas.put(key, new Property() {
                        @Override
                        public Thing getThing() {
                            return p;
                        }

                        @Override
                        public String getKey() {
                            return aKey;
                        }

                        @Override
                        public Object getValue() throws NoSuchElementException {
                            return aValue;
                        }

                        @Override
                        public boolean isPresent() {
                            return null != aValue;
                        }

                        @Override
                        public Property setProperty(String key, Object value) throws IllegalStateException {
                            throw new IllegalStateException("A meta property can not have a meta property");
                        }

                        @Override
                        public Property getProperty(String key) throws IllegalStateException {
                            throw new IllegalStateException("A meta property can not have a meta property");
                        }

                        @Override
                        public Property removeProperty(String key) throws IllegalStateException {
                            throw new IllegalStateException("A meta property can not have a meta property");
                        }
                    });
                    return null == property ? Property.empty() : property;
                }

                public Property getProperty(String key) throws IllegalStateException {
                    final Property property = this.metas.get(key);
                    return null == property ? Property.empty() : property;
                }

                public Property removeProperty(String key) throws IllegalStateException {
                    final Property property = this.metas.remove(key);
                    return null == property ? Property.empty() : property;
                }
            };
        }
        return properties;
    }

    public static <T, E extends Thing> Property<T, E> empty() {
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
            public E getThing() {
                throw new IllegalStateException(EMPTY_MESSAGE);
            }

            @Override
            public <R> Property<R, Property> setProperty(String key, R value) throws IllegalStateException {
                throw new IllegalStateException(EMPTY_MESSAGE);
            }

            @Override
            public <R> Property<R, Property> getProperty(String key) throws IllegalStateException {
                throw new IllegalStateException(EMPTY_MESSAGE);
            }

            @Override
            public <R> Property<R, Property> removeProperty(String key) throws IllegalStateException {
                throw new IllegalStateException(EMPTY_MESSAGE);
            }
        };
    }
}
