package com.tinkerpop.blueprints;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Element extends Thing, Featureable {

    public Object getId();

    public <V> Property<V, ? extends Element> getProperty(String key);

    public <V> Property<V, ? extends Element> setProperty(String key, V value);

    public <V> Property<V, ? extends Element> removeProperty(String key);

    public void remove();


    public interface Features extends com.tinkerpop.blueprints.Features {

        public static IllegalArgumentException bothIsNotSupported() {
            return new IllegalArgumentException("A direction of BOTH is not supported");
        }
    }

}
