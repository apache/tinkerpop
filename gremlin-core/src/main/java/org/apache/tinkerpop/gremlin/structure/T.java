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

import java.util.function.Function;

/**
 * A collection of (T)okens which allows for more concise Traversal definitions.
 * T implements {@link Function} can be used to map an element to its token value.
 * For example, <code>T.id.apply(element)</code>.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public enum T implements Function<Element, Object> {
    /**
     * Label (representing Element.label())
     *
     * @since 3.0.0-incubating
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
     *
     * @since 3.0.0-incubating
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
     *
     * @since 3.0.0-incubating
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
     *
     * @since 3.0.0-incubating
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

    private static final String LABEL = Graph.Hidden.hide("label");
    private static final String ID = Graph.Hidden.hide("id");
    private static final String KEY = Graph.Hidden.hide("key");
    private static final String VALUE = Graph.Hidden.hide("value");

    public abstract String getAccessor();

    @Override
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
            throw new IllegalArgumentException("The following token string is unknown: " + accessor);
    }
}
