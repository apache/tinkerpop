package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.MetaProperty;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MetaPropertyStep<E> extends FlatMapStep<Vertex, MetaProperty<E>> { // TODO: implements Reversible {

    public String[] metaPropertyKeys;

    public MetaPropertyStep(final Traversal traversal, final String... metaPropertyKeys) {
        super(traversal);
        this.metaPropertyKeys = metaPropertyKeys;
        this.setFunction(traverser -> (Iterator) traverser.get().properties());
    }

    public String toString() {
        return this.metaPropertyKeys.length == 0 ? super.toString() : TraversalHelper.makeStepString(this, Arrays.asList(this.metaPropertyKeys));
    }
}
