/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.example.gadget;

import java.util.Objects;

/**
 * Test-only types whose names are not among the GraphSON 1.0 allowed type names, used to exercise the GraphSON 1.0
 * embedded-type rules and the {@code addAllowedTypeIdName(...)} opt-out.
 */
public final class GraphSONTestGadgets {

    private GraphSONTestGadgets() {
    }

    /**
     * Records that its static initializer ran through a system property, so a test can observe initialization
     * without referencing the class, which would itself trigger it.
     */
    public static class StaticInitCanary {
        public static final String FIRED_PROPERTY = "tinkerpop.test.graphson.staticInitCanary";
        static {
            System.setProperty(FIRED_PROPERTY, "fired");
        }
        public int x;
    }

    /**
     * Counterpart of {@link StaticInitCanary} for a class named by a collection element.
     * Each scenario needs a distinct canary because a class's static initializer runs only once per JVM.
     */
    public static class StaticInitCanaryElement {
        public static final String FIRED_PROPERTY = "tinkerpop.test.graphson.staticInitCanaryElement";
        static {
            System.setProperty(FIRED_PROPERTY, "fired");
        }
        public int x;
    }

    /**
     * A plain bean with no static-initializer side effect, used to check the opt-out.
     */
    public static class SamplePojo {
        public int x;

        public SamplePojo() {
        }

        public SamplePojo(final int x) {
            this.x = x;
        }

        @Override
        public boolean equals(final Object o) {
            return o instanceof SamplePojo && ((SamplePojo) o).x == this.x;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(x);
        }
    }

    /**
     * A subclass of {@link SamplePojo}, used to verify that configuring the base class name does not permit its
     * subclasses.
     */
    public static class SamplePojoSubclass extends SamplePojo {
        public SamplePojoSubclass() {
        }

        public SamplePojoSubclass(final int x) {
            super(x);
        }
    }

    /**
     * Counterpart of {@link StaticInitCanary} for an enum named as a generic type argument, which Jackson does not
     * otherwise validate.
     */
    public enum StaticInitCanaryEnum {
        A, B;
        public static final String FIRED_PROPERTY = "tinkerpop.test.graphson.staticInitCanaryEnum";
        static {
            System.setProperty(FIRED_PROPERTY, "fired");
        }
    }

    /**
     * Counterpart of {@link StaticInitCanary} for a class named as a generic type argument.
     * Each scenario needs a distinct canary because a class's static initializer runs only once per JVM.
     */
    public static class StaticInitCanaryArg {
        public static final String FIRED_PROPERTY = "tinkerpop.test.graphson.staticInitCanaryArg";
        static {
            System.setProperty(FIRED_PROPERTY, "fired");
        }
        public int x;
    }

    /**
     * Counterpart of {@link StaticInitCanary} for the class a {@code java.lang.Class} value names.
     */
    public static class StaticInitCanaryValue {
        public static final String FIRED_PROPERTY = "tinkerpop.test.graphson.staticInitCanaryValue";
        static {
            System.setProperty(FIRED_PROPERTY, "fired");
        }
        public int x;
    }
}
