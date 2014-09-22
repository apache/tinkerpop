package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.structure.Compare;
import com.tinkerpop.gremlin.structure.Contains;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.util.function.SBiPredicate;

import java.util.Comparator;

/**
 * A collection of (T)okens which allows for more concise Traversal definitions.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public enum T {
    /**
     * Greater than
     */
    gt {
        public SBiPredicate getPredicate() {
            return Compare.GREATER_THAN;
        }
    },
    /**
     * Less than
     */
    lt {
        public SBiPredicate getPredicate() {
            return Compare.LESS_THAN;
        }
    },
    /**
     * Equal to
     */
    eq {
        public SBiPredicate getPredicate() {
            return Compare.EQUAL;
        }
    },
    /**
     * Greater than or equal to
     */
    gte {
        public SBiPredicate getPredicate() {
            return Compare.GREATER_THAN_EQUAL;
        }
    },
    /**
     * Less than or equal to
     */
    lte {
        public SBiPredicate getPredicate() {
            return Compare.LESS_THAN_EQUAL;
        }
    },
    /**
     * Not equal to
     */
    neq {
        public SBiPredicate getPredicate() {
            return Compare.NOT_EQUAL;
        }
    },
    /**
     * Decrement
     */
    decr {
        public Comparator getComparator() {
            return Comparator.reverseOrder();
        }
    },
    /**
     * Increment
     */
    incr {
        public Comparator getComparator() {
            return Comparator.naturalOrder();
        }
    },
    /**
     * In collection
     */
    in {
        public SBiPredicate getPredicate() {
            return Contains.IN;
        }
    },
    /**
     * Not in collection
     */
    nin {
        public SBiPredicate getPredicate() {
            return Contains.NOT_IN;
        }
    },
    /**
     * Label (representing Element.label())
     */
    label {
        public String getAccessor() {
            return LABEL;
        }
    },
    /**
     * Id (representing Element.id())
     */
    id {
        public String getAccessor() {
            return ID;
        }
    },
    /**
     * Key (representing Property.key())
     */
    key {
        public String getAccessor() {
            return KEY;
        }
    },
    /**
     * Value (representing Property.value())
     */
    value {
        public String getAccessor() {
            return VALUE;
        }
    };

    private static final String LABEL = Graph.System.system("label");
    private static final String ID = Graph.System.system("id");
    private static final String KEY = Graph.System.system("key");
    private static final String VALUE = Graph.System.system("value");


    public SBiPredicate getPredicate() {
        throw new IllegalArgumentException(this.toString() + " is an unknown predicate type");
    }

    public Comparator getComparator() {
        throw new IllegalArgumentException(this.toString() + " is an unknown comparator type");
    }

    public String getAccessor() {
        throw new IllegalArgumentException(this.toString() + " is an unknown accessor type");
    }

}
