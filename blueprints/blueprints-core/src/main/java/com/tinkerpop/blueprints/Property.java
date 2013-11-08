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
public interface Property<V, T extends Thing> extends Element {

    public enum Key {
        ID, LABEL;

        private static final String LABEL_STRING = "label";
        private static final String ID_STRING = "id";
        private static final String HIDDEN_PREFIX = "%&%";

        public String toString() {
            if (this == ID) {
                return ID_STRING;
            } else {
                return LABEL_STRING;
            }
        }

        public static String hidden(final String key) {
            return HIDDEN_PREFIX.concat(key);
        }
    }

    public T getThing();

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

    public <V2> Property<V2, Property> setProperty(String key, V2 value) throws IllegalStateException;

    public <V2> Property<V2, Property> getProperty(String key) throws IllegalStateException;

    public <V2> Property<V2, Property> removeProperty(String key) throws IllegalStateException;

    public default void remove() {
        this.getThing().removeProperty(this.getKey());
    }

    public default Object getId() {
        return this.hashCode();
    }

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

        public static NoSuchElementException propertyHasNoValue() {
            throw new NoSuchElementException("The property has no value and thus, does not exist");
        }
    }

    public static Property[] of(Object... keyValues) {
        if (keyValues.length % 2 != 0)
            throw new IllegalArgumentException("The provided arguments must have a size that is a factor of 2");
        final Property[] properties = new Property[keyValues.length / 2];
        for (int i = 0; i < keyValues.length; i = i + 2) {
            final String key = Objects.requireNonNull(keyValues[i]).toString();
            final Object value = Objects.requireNonNull(keyValues[i + 1]);

            properties[i / 2] = new Property() {
                final Map<String, Property> properties = new HashMap<>();
                final Property p = this;

                @Override
                public Map<String, Property> getProperties() {
                    return new HashMap<>(this.properties);
                }

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
                    final Property property = this.properties.put(key, new Property() {
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
                            throw Features.propertyPropertyCanNotHaveAProperty();
                        }

                        @Override
                        public Property getProperty(String key) throws IllegalStateException {
                            throw Features.propertyPropertyCanNotHaveAProperty();
                        }

                        @Override
                        public Property removeProperty(String key) throws IllegalStateException {
                            throw Features.propertyPropertyCanNotHaveAProperty();
                        }

                        @Override
                        public Map<String, Property> getProperties() {
                            throw Features.propertyPropertyCanNotHaveAProperty();
                        }
                    });
                    return null == property ? Property.empty() : property;
                }

                public Property getProperty(String key) throws IllegalStateException {
                    final Property property = this.properties.get(key);
                    return null == property ? Property.empty() : property;
                }

                public Property removeProperty(String key) throws IllegalStateException {
                    final Property property = this.properties.remove(key);
                    return null == property ? Property.empty() : property;
                }
            };
        }
        return properties;
    }

    public static <V, T extends Thing> Property<V, T> empty() {
        return new Property<V, T>() {
            private static final String EMPTY_KEY = "empty";
            private static final String EMPTY_MESSAGE = "This is an empty property";

            @Override
            public String getKey() {
                return EMPTY_KEY;
            }

            @Override
            public V getValue() throws NoSuchElementException {
                throw Features.propertyHasNoValue();
            }

            @Override
            public boolean isPresent() {
                return false;
            }

            @Override
            public T getThing() {
                throw new IllegalStateException(EMPTY_MESSAGE);
            }

            @Override
            public <V2> Property<V2, Property> setProperty(String key, V2 value) throws IllegalStateException {
                throw new IllegalStateException(EMPTY_MESSAGE);
            }

            @Override
            public <V2> Property<V2, Property> getProperty(String key) throws IllegalStateException {
                throw new IllegalStateException(EMPTY_MESSAGE);
            }

            @Override
            public <V2> Property<V2, Property> removeProperty(String key) throws IllegalStateException {
                throw new IllegalStateException(EMPTY_MESSAGE);
            }

            @Override
            public Map<String, Property> getProperties() throws IllegalStateException {
                throw new IllegalStateException(EMPTY_MESSAGE);
            }
        };
    }
}
