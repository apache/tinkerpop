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
package org.apache.tinkerpop.gremlin.structure.util.batch;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

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
            ElementHelper.attachProperties(element, keyValues);
        }
    },
    OVERWRITE_SINGLE {
        @Override
        public void accept(final Element element, final Object[] keyValues) {
            if (element instanceof Vertex)
                ElementHelper.attachSingleProperties((Vertex) element, keyValues);
            else
                ElementHelper.attachProperties(element, keyValues);
        }
    }
}
