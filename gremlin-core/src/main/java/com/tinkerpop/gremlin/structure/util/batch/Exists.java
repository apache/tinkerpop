package com.tinkerpop.gremlin.structure.util.batch;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.function.BiConsumer;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public enum Exists implements BiConsumer<Element, Object[]> {
    IGNORE {
        @Override
        public void accept(final Element element, final Object[] objects) {
            // do nothing
        }
    },
    THROW {
        @Override
        public void accept(final Element element, final Object[] objects) {
            throw new IllegalStateException(String.format(
                    "Element of type %s with id of [%s] was not expected to exist in target graph",
                    element.getClass().getSimpleName(), element.id()));
        }
    },
    OVERWRITE {
        @Override
        public void accept(final Element element, final Object[] keyValues) {
            // TODO: if (element instanceof Vertex) {
            // remove properties if overwrite
            // } else
            ElementHelper.attachProperties(element, keyValues);
        }
    }
}
