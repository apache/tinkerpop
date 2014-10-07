package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Element;

import java.util.Optional;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ValueStep<E> extends MapStep<Element, E> {

    private final String key;
    private final Optional<E> defaultValue;
    private final Optional<Supplier<E>> defaultSupplier;

    public ValueStep(final Traversal traversal, final String key) {
        super(traversal);
        this.key = key;
        this.defaultValue = Optional.empty();
        this.defaultSupplier = Optional.empty();
        this.setFunction(traverser -> traverser.get().<E>property(key).orElse((E) NO_OBJECT));
    }

    public ValueStep(final Traversal traversal, final String key, final E defaultValue) {
        super(traversal);
        this.key = key;
        this.defaultValue = Optional.of(defaultValue);
        this.defaultSupplier = Optional.empty();
        this.setFunction(traverser -> traverser.get().<E>property(key).orElse(this.defaultValue.get()));
    }

    public ValueStep(final Traversal traversal, final String key, final Supplier<E> defaultSupplier) {
        super(traversal);
        this.key = key;
        this.defaultValue = Optional.empty();
        this.defaultSupplier = Optional.of(defaultSupplier);
        this.setFunction(traverser -> traverser.get().<E>property(key).orElse(this.defaultSupplier.get().get()));
    }

    public String toString() {
        return TraversalHelper.makeStepString(this, this.key);
    }
}
