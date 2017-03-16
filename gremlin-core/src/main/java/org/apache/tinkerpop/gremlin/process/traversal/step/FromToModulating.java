/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.process.traversal.step;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;

/**
 * FromToModulating are for {@link org.apache.tinkerpop.gremlin.process.traversal.Step}s that support from()- and to()-modulation.
 * This step is similar to {@link ByModulating}.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface FromToModulating {

    public default void addFrom(final Traversal.Admin<?, ?> fromTraversal) {
        throw new UnsupportedOperationException("The from()-modulating step does not support traversal-based modulation: " + this);
    }

    public default void addTo(final Traversal.Admin<?, ?> toTraversal) {
        throw new UnsupportedOperationException("The to()-modulating step does not support traversal-based modulation: " + this);
    }

    public default void addFrom(final String fromLabel) {
        addFrom(__.select(fromLabel).asAdmin());
    }

    public default void addTo(final String toLabel) {
        addTo(__.select(toLabel).asAdmin());
    }
}
