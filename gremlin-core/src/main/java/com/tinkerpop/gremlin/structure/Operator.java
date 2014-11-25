package com.tinkerpop.gremlin.structure;

import java.util.function.BinaryOperator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public enum Operator implements BinaryOperator<Number> {


    sum {
        public Number apply(final Number a, final Number b) {
            final Class<? extends Number> numberClass = a.getClass();
            if (numberClass.equals(Integer.class)) {
                return a.intValue() + b.intValue();
            } else if (numberClass.equals(Long.class)) {
                return a.longValue() + b.longValue();
            } else if (numberClass.equals(Float.class)) {
                return a.floatValue() + b.floatValue();
            } else if (numberClass.equals(Double.class)) {
                return a.doubleValue() + b.doubleValue();
            } else {
                throw new IllegalArgumentException("This operator only supports int, long, float, or double: " + numberClass);
            }
        }
    },
    minus {
        public Number apply(final Number a, final Number b) {
            final Class<? extends Number> numberClass = a.getClass();
            if (numberClass.equals(Integer.class)) {
                return a.intValue() - b.intValue();
            } else if (numberClass.equals(Long.class)) {
                return a.longValue() - b.longValue();
            } else if (numberClass.equals(Float.class)) {
                return a.floatValue() - b.floatValue();
            } else if (numberClass.equals(Double.class)) {
                return a.doubleValue() - b.doubleValue();
            } else {
                throw new IllegalArgumentException("This operator only supports int, long, float, or double: " + numberClass);
            }
        }
    },
    mult {
        public Number apply(final Number a, final Number b) {
            final Class<? extends Number> numberClass = a.getClass();
            if (numberClass.equals(Integer.class)) {
                return a.intValue() * b.intValue();
            } else if (numberClass.equals(Long.class)) {
                return a.longValue() * b.longValue();
            } else if (numberClass.equals(Float.class)) {
                return a.floatValue() * b.floatValue();
            } else if (numberClass.equals(Double.class)) {
                return a.doubleValue() * b.doubleValue();
            } else {
                throw new IllegalArgumentException("This operator only supports int, long, float, or double: " + numberClass);
            }
        }
    },
    div {
        public Number apply(final Number a, final Number b) {
            final Class<? extends Number> numberClass = a.getClass();
            if (numberClass.equals(Integer.class)) {
                return a.intValue() / b.intValue();
            } else if (numberClass.equals(Long.class)) {
                return a.longValue() / b.longValue();
            } else if (numberClass.equals(Float.class)) {
                return a.floatValue() / b.floatValue();
            } else if (numberClass.equals(Double.class)) {
                return a.doubleValue() / b.doubleValue();
            } else {
                throw new IllegalArgumentException("This operator only supports int, long, float, or double: " + numberClass);
            }
        }
    },
    min {
        public Number apply(final Number a, final Number b) {
            final Class<? extends Number> numberClass = a.getClass();
            if (numberClass.equals(Integer.class)) {
                return Math.min(a.intValue(), b.intValue());
            } else if (numberClass.equals(Long.class)) {
                return Math.min(a.longValue(), b.longValue());
            } else if (numberClass.equals(Float.class)) {
                return Math.min(a.floatValue(), b.floatValue());
            } else if (numberClass.equals(Double.class)) {
                return Math.min(a.doubleValue(), b.doubleValue());
            } else {
                throw new IllegalArgumentException("This operator only supports int, long, float, or double: " + numberClass);
            }
        }
    },
    max {
        public Number apply(final Number a, final Number b) {
            final Class<? extends Number> numberClass = a.getClass();
            if (numberClass.equals(Integer.class)) {
                return Math.max(a.intValue(), b.intValue());
            } else if (numberClass.equals(Long.class)) {
                return Math.max(a.longValue(), b.longValue());
            } else if (numberClass.equals(Float.class)) {
                return Math.max(a.floatValue(), b.floatValue());
            } else if (numberClass.equals(Double.class)) {
                return Math.max(a.doubleValue(), b.doubleValue());
            } else {
                throw new IllegalArgumentException("This operator only supports int, long, float, or double: " + numberClass);
            }
        }
    }
}
