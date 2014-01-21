package com.tinkerpop.gremlin.oltp.map;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.gremlin.MapPipe;
import com.tinkerpop.gremlin.Pipeline;

import java.util.Optional;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ValuePipe<E> extends MapPipe<Element, E> {

    public String key;
    public Optional<E> defaultValue;
    public Optional<Supplier<E>> defaultSupplier;

    public ValuePipe(final Pipeline pipeline, final String key) {
        super(pipeline);
        this.key = key;
        this.defaultValue = Optional.empty();
        this.defaultSupplier = Optional.empty();
        this.setFunction(holder -> holder.get().<E>getValue(key));
    }

    public ValuePipe(final Pipeline pipeline, final String key, final E defaultValue) {
        super(pipeline);
        this.key = key;
        this.defaultValue = Optional.of(defaultValue);
        this.defaultSupplier = Optional.empty();
        this.setFunction(holder -> holder.get().<E>getProperty(key).orElse(this.defaultValue.get()));
    }

    public ValuePipe(final Pipeline pipeline, final String key, final Supplier<E> defaultSupplier) {
        super(pipeline);
        this.key = key;
        this.defaultValue = Optional.empty();
        this.defaultSupplier = Optional.of(defaultSupplier);
        this.setFunction(holder -> holder.get().<E>getProperty(key).orElse(this.defaultSupplier.get().get()));
    }
}
