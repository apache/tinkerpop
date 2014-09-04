package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.util.SOptional;

import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HiddenStep<E> extends MapStep<Element, E> {

    public String key;
    public SOptional<E> defaultValue;
    public SOptional<Supplier<E>> defaultSupplier;

    public HiddenStep(final Traversal traversal, final String key) {
        super(traversal);
        this.key = Graph.Key.hide(key);
        this.defaultValue = SOptional.empty();
        this.defaultSupplier = SOptional.empty();
        this.setFunction(traverser -> traverser.get().<E>property(key).orElse((E) NO_OBJECT));
    }

    public HiddenStep(final Traversal traversal, final String key, final E defaultValue) {
        super(traversal);
        this.key = Graph.Key.hide(key);
        this.defaultValue = SOptional.of(defaultValue);
        this.defaultSupplier = SOptional.empty();
        this.setFunction(traverser -> traverser.get().<E>property(key).orElse(this.defaultValue.get()));
    }

    public HiddenStep(final Traversal traversal, final String key, final Supplier<E> defaultSupplier) {
        super(traversal);
        this.key = Graph.Key.hide(key);
        this.defaultValue = SOptional.empty();
        this.defaultSupplier = SOptional.of(defaultSupplier);
        this.setFunction(traverser -> traverser.get().<E>property(key).orElse(this.defaultSupplier.get().get()));
    }

    public String toString() {
        return TraversalHelper.makeStepString(this, this.key);
    }
}
