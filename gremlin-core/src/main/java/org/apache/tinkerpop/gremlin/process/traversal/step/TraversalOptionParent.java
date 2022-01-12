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
package org.apache.tinkerpop.gremlin.process.traversal.step;

import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.process.traversal.Pick;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;

/**
 * Describes steps that can be parent to a {@link Traversal} from the {@code option()} modulator.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface TraversalOptionParent<M, S, E> extends TraversalParent {

    /**
     * The child as defined by the token it takes, like {@link Pick} or {@link Merge}. This traversal may be of local
     * or global scope depending on the step implementation that works with {@code option()}.
     */
    public void addChildOption(final M token, final Traversal.Admin<S, E> traversalOption);
}
