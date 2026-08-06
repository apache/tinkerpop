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
 * Test-only types in a package outside every allowlisted GraphSON 1.0 type prefix, used to exercise the
 * GraphSON 1.0 embedded-type restriction and its {@code addAllowedTypeIdPrefix(...)} opt-out.
 */
public final class GraphSONTestGadgets {

    private GraphSONTestGadgets() {
    }

    /**
     * Records execution of its static initializer through a system property, so a test can observe whether the
     * class was loaded/initialized without referencing it (which would itself trigger initialization).
     */
    public static class StaticInitCanary {
        public static final String FIRED_PROPERTY = "tinkerpop.test.graphson.staticInitCanary";
        static {
            System.setProperty(FIRED_PROPERTY, "fired");
        }
        public int x;
    }

    /**
     * A plain bean with no static-initializer side effect, used to verify the opt-out re-enables a trusted type.
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
     * Enum counterpart of {@link StaticInitCanary}, used to verify an enum named as a generic type argument is
     * refused before it is loaded (Jackson otherwise skips validation of enum type arguments).
     */
    public enum StaticInitCanaryEnum {
        A, B;
        public static final String FIRED_PROPERTY = "tinkerpop.test.graphson.staticInitCanaryEnum";
        static {
            System.setProperty(FIRED_PROPERTY, "fired");
        }
    }

    /**
     * Canary used to verify a disallowed class named as a generic type argument is not loaded when refused.
     */
    public static class StaticInitCanaryArg {
        public static final String FIRED_PROPERTY = "tinkerpop.test.graphson.staticInitCanaryArg";
        static {
            System.setProperty(FIRED_PROPERTY, "fired");
        }
        public int x;
    }

    /**
     * Canary used to verify a java.lang.Class value cannot name and load an arbitrary class.
     */
    public static class StaticInitCanaryValue {
        public static final String FIRED_PROPERTY = "tinkerpop.test.graphson.staticInitCanaryValue";
        static {
            System.setProperty(FIRED_PROPERTY, "fired");
        }
        public int x;
    }
}
