package com.tinkerpop.gremlin.process.steps.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Element;

import java.util.Optional;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PropertyValueStep<E> extends MapStep<Element, E> {

    public String key;
    public Optional<E> defaultValue;
    public Optional<Supplier<E>> defaultSupplier;

    public PropertyValueStep(final Traversal traversal, final String key) {
        super(traversal);
        this.key = key;
        this.defaultValue = Optional.empty();
        this.defaultSupplier = Optional.empty();
        this.setFunction(holder -> holder.get().<E>getProperty(key).orElse((E) NO_OBJECT));
    }

    public PropertyValueStep(final Traversal traversal, final String key, final E defaultValue) {
        super(traversal);
        this.key = key;
        this.defaultValue = Optional.of(defaultValue);
        this.defaultSupplier = Optional.empty();
        this.setFunction(holder -> holder.get().<E>getProperty(key).orElse(this.defaultValue.get()));
    }

    public PropertyValueStep(final Traversal traversal, final String key, final Supplier<E> defaultSupplier) {
        super(traversal);
        this.key = key;
        this.defaultValue = Optional.empty();
        this.defaultSupplier = Optional.of(defaultSupplier);
        this.setFunction(holder -> holder.get().<E>getProperty(key).orElse(this.defaultSupplier.get().get()));
    }
}
