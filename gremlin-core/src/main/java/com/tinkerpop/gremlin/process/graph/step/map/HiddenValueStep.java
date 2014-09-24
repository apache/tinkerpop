package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;

import java.util.Optional;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HiddenValueStep<E> extends MapStep<Element, E> {

    private boolean isHidden;
    public String key;
    public String hiddenKey;
    public Optional<E> defaultValue;
    public Optional<Supplier<E>> defaultSupplier;

    public HiddenValueStep(final Traversal traversal, final String key) {
        super(traversal);
        this.key = key;
        this.isHidden = Graph.Key.isHidden(this.key);
        this.hiddenKey = Graph.Key.hide(this.key);
        this.defaultValue = Optional.empty();
        this.defaultSupplier = Optional.empty();
        this.setFunction(traverser -> this.isHidden ? (E) NO_OBJECT : traverser.get().<E>property(this.hiddenKey).orElse((E) NO_OBJECT));
    }

    public HiddenValueStep(final Traversal traversal, final String key, final E defaultValue) {
        super(traversal);
        this.key = key;
        this.isHidden = Graph.Key.isHidden(this.key);
        this.hiddenKey = Graph.Key.hide(this.key);
        this.defaultValue = Optional.of(defaultValue);
        this.defaultSupplier = Optional.empty();
        this.setFunction(traverser -> this.isHidden ? (E) NO_OBJECT : traverser.get().<E>property(this.hiddenKey).orElse(this.defaultValue.get()));
    }

    public HiddenValueStep(final Traversal traversal, final String key, final Supplier<E> defaultSupplier) {
        super(traversal);
        this.key = key;
        this.isHidden = Graph.Key.isHidden(this.key);
        this.hiddenKey = Graph.Key.hide(this.key);
        this.defaultValue = Optional.empty();
        this.defaultSupplier = Optional.of(defaultSupplier);
        this.setFunction(traverser -> this.isHidden ? (E) NO_OBJECT : traverser.get().<E>property(this.hiddenKey).orElse(this.defaultSupplier.get().get()));
    }

    public String toString() {
        return TraversalHelper.makeStepString(this, this.key);
    }
}
