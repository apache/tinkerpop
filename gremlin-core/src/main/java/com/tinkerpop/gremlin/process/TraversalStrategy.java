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
package com.tinkerpop.gremlin.process;

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;

/**
 * A {@link TraversalStrategy} defines a particular atomic operation for mutating a {@link Traversal} prior to its evaluation.
 * Traversal strategies are typically used for optimizing a traversal for the particular underlying graph engine.
 * Traversal strategies implement {@link Comparable} and thus are sorted to determine their evaluation order.
 * A TraversalStrategy should not have a public constructor as they should not maintain state between applications.
 * Make use of a singleton instance() object to reduce object creation on the JVM.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface TraversalStrategy extends Serializable{

    // A TraversalStrategy should not have a public constructor
    // Make use of a singleton instance() object to reduce object creation on the JVM
    // Moreover they are stateless objects.

    public void apply(final Traversal.Admin<?, ?> traversal, final TraversalEngine traversalEngine);

    public default Set<Class<? extends TraversalStrategy>> applyPrior() {
        return Collections.emptySet();
    }

    public default Set<Class<? extends TraversalStrategy>> applyPost() {
        return Collections.emptySet();
    }

}
