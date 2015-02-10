/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.structure;

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
