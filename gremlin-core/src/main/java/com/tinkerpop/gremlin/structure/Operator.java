package com.tinkerpop.gremlin.structure;

import java.util.Collection;
import java.util.function.BinaryOperator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public enum Operator implements BinaryOperator<Object> {


    sum {
        public Object apply(final Object a, final Object b) {
            final Class objectClass = a.getClass();
            if (objectClass.equals(Integer.class)) {
                return ((Number) a).intValue() + ((Number) b).intValue();
            } else if (objectClass.equals(Long.class)) {
                return ((Number) a).longValue() + ((Number) b).longValue();
            } else if (objectClass.equals(Float.class)) {
                return ((Number) a).floatValue() + ((Number) b).floatValue();
            } else if (objectClass.equals(Double.class)) {
                return ((Number) a).doubleValue() + ((Number) b).doubleValue();
            } else if (Collection.class.isAssignableFrom(objectClass)) {
                ((Collection) a).addAll((Collection) b);
                return a;
            } else {
                throw new IllegalArgumentException("This operator only supports int, long, float, double, or collection: " + objectClass);
            }
        }
    },
    minus {
        public Object apply(final Object a, final Object b) {
            final Class objectClass = a.getClass();
            if (objectClass.equals(Integer.class)) {
                return ((Number) a).intValue() - ((Number) b).intValue();
            } else if (objectClass.equals(Long.class)) {
                return ((Number) a).longValue() - ((Number) b).longValue();
            } else if (objectClass.equals(Float.class)) {
                return ((Number) a).floatValue() - ((Number) b).floatValue();
            } else if (objectClass.equals(Double.class)) {
                return ((Number) a).doubleValue() - ((Number) b).doubleValue();
            } else if (Collection.class.isAssignableFrom(objectClass)) {
                ((Collection) a).removeAll((Collection) b);
                return a;
            } else {
                throw new IllegalArgumentException("This operator only supports int, long, float, double, or collection: " + objectClass);
            }
        }
    },
    mult {
        public Object apply(final Object a, final Object b) {
            final Class objectClass = a.getClass();
            if (objectClass.equals(Integer.class)) {
                return ((Number) a).intValue() * ((Number) b).intValue();
            } else if (objectClass.equals(Long.class)) {
                return ((Number) a).longValue() * ((Number) b).longValue();
            } else if (objectClass.equals(Float.class)) {
                return ((Number) a).floatValue() * ((Number) b).floatValue();
            } else if (objectClass.equals(Double.class)) {
                return ((Number) a).doubleValue() * ((Number) b).doubleValue();
            } else {
                throw new IllegalArgumentException("This operator only supports int, long, float, or double: " + objectClass);
            }
        }
    },
    div {
        public Object apply(final Object a, final Object b) {
            final Class objectClass = a.getClass();
            if (objectClass.equals(Integer.class)) {
                return ((Number) a).intValue() / ((Number) b).intValue();
            } else if (objectClass.equals(Long.class)) {
                return ((Number) a).longValue() / ((Number) b).longValue();
            } else if (objectClass.equals(Float.class)) {
                return ((Number) a).floatValue() / ((Number) b).floatValue();
            } else if (objectClass.equals(Double.class)) {
                return ((Number) a).doubleValue() / ((Number) b).doubleValue();
            } else {
                throw new IllegalArgumentException("This operator only supports int, long, float, or double: " + objectClass);
            }
        }
    },
    min {
        public Object apply(final Object a, final Object b) {
            final Class objectClass = a.getClass();
            if (objectClass.equals(Integer.class)) {
                return Math.min(((Number) a).intValue(), ((Number) b).intValue());
            } else if (objectClass.equals(Long.class)) {
                return Math.min(((Number) a).longValue(), ((Number) b).longValue());
            } else if (objectClass.equals(Float.class)) {
                return Math.min(((Number) a).floatValue(), ((Number) b).floatValue());
            } else if (objectClass.equals(Double.class)) {
                return Math.min(((Number) a).doubleValue(), ((Number) b).doubleValue());
            } else {
                throw new IllegalArgumentException("This operator only supports int, long, float, or double: " + objectClass);
            }
        }
    },
    max {
        public Object apply(final Object a, final Object b) {
            final Class objectClass = a.getClass();
            if (objectClass.equals(Integer.class)) {
                return Math.max(((Number) a).intValue(), ((Number) b).intValue());
            } else if (objectClass.equals(Long.class)) {
                return Math.max(((Number) a).longValue(), ((Number) b).longValue());
            } else if (objectClass.equals(Float.class)) {
                return Math.max(((Number) a).floatValue(), ((Number) b).floatValue());
            } else if (objectClass.equals(Double.class)) {
                return Math.max(((Number) a).doubleValue(), ((Number) b).doubleValue());
            } else {
                throw new IllegalArgumentException("This operator only supports int, long, float, or double: " + objectClass);
            }
        }
    };

    public <M> BinaryOperator<M> type() {
        return (BinaryOperator) this;
    }
}

