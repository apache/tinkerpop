package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.VertexProperty;

import java.util.function.Function;

/**
 * A collection of (T)okens which allows for more concise Traversal definitions.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public enum T implements Function<Element,Object> {
    /**
     * Label (representing Element.label())
     */
    label {
        @Override
        public String getAccessor() {
            return LABEL;
        }

        @Override
        public String apply(final Element element) {
            return element.label();
        }
    },
    /**
     * Id (representing Element.id())
     */
    id {
        @Override
        public String getAccessor() {
            return ID;
        }

        @Override
        public Object apply(final Element element) {
            return element.id();
        }
    },
    /**
     * Key (representing Property.key())
     */
    key {
        @Override
        public String getAccessor() {
            return KEY;
        }

        @Override
        public String apply(final Element element) {
            return ((VertexProperty) element).key();
        }
    },
    /**
     * Value (representing Property.value())
     */
    value {
        @Override
        public String getAccessor() {
            return VALUE;
        }

        @Override
        public Object apply(final Element element) {
            return ((VertexProperty) element).value();
        }
    };

    private static final String LABEL = Graph.System.system("label");
    private static final String ID = Graph.System.system("id");
    private static final String KEY = Graph.System.system("key");
    private static final String VALUE = Graph.System.system("value");

    public abstract String getAccessor();

    public abstract Object apply(final Element element);

    public static T fromString(final String accessor) {
        if (accessor.equals(LABEL))
            return label;
        else if (accessor.equals(ID))
            return id;
        else if (accessor.equals(KEY))
            return key;
        else if (accessor.equals(VALUE))
            return value;
        else
            throw new IllegalArgumentException("The following accessor string is unknown: " + accessor);
    }

}
