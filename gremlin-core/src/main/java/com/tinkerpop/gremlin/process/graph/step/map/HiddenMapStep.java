package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HiddenMapStep<E> extends MapStep<Element, Map<String, E>> {

    public String[] hiddenPropertyKeys;

    public HiddenMapStep(final Traversal traversal, final String... propertyKeys) {
        super(traversal);
        this.hiddenPropertyKeys = propertyKeys;

        // convert keys to hidden transparently
        for (int i = 0; i < propertyKeys.length; i++) {
            this.hiddenPropertyKeys[i] = Graph.Key.hide(this.hiddenPropertyKeys[i]);
        }

        this.setFunction(traverser -> {
            final Element element = traverser.get();

            // if no keys were supplied to the step then all the keys are required.  in that case the hidden keys
            // need to be identified from the Element
            final String[] hiddens = null == this.hiddenPropertyKeys || this.hiddenPropertyKeys.length == 0 ?
                    element.hiddenKeys().toArray(new String[element.hiddenKeys().size()]) : this.hiddenPropertyKeys;

            // since the hidden keys need to be explicitly identified there is no need to built a Map if none are
            // in the hiddens String array.
            return hiddens.length == 0 ?
                    Collections.emptyMap() : element instanceof Vertex ?
                    (Map) ElementHelper.metaPropertyMap((Vertex) traverser.get(), hiddens) :
                    (Map) ElementHelper.propertyMap(traverser.get(), hiddens);
        });
    }

    public String toString() {
        return TraversalHelper.makeStepString(this, Arrays.asList(this.hiddenPropertyKeys));
    }
}
