package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.structure.Graph;

/**
 * A collection of (T)okens which allows for more concise Traversal definitions.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public enum T {
    /**
     * Label (representing Element.label())
     */
    label {
        @Override
        public String getAccessor() {
            return LABEL;
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
    },
    /**
     * Key (representing Property.key())
     */
    key {
        @Override
        public String getAccessor() {
            return KEY;
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
    };

    private static final String LABEL = Graph.System.system("label");
    private static final String ID = Graph.System.system("id");
    private static final String KEY = Graph.System.system("key");
    private static final String VALUE = Graph.System.system("value");

    public abstract String getAccessor();

}
