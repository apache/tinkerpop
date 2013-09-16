package com.tinkerpop.blueprints;

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

    public T getValue();

    public <T> void setMetaValue(String key, T value);

    public <T> T getMetaValue(String key);

    public <T> T removeMetaValue(String key);

    public static Property[] make(Object... values) {
        throw new UnsupportedOperationException();
    }
}
