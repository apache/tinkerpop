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
package org.apache.tinkerpop.gremlin.process;

import org.apache.tinkerpop.gremlin.util.InterruptedRuntimeException;

import java.util.Optional;

/**
 * An unchecked exception thrown when the current thread processing a {@link Traversal} is interrupted.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class TraversalInterruptedException extends InterruptedRuntimeException {
    private final Optional<Traversal> interruptedTraversal;

    public TraversalInterruptedException(final Throwable t) {
        this(null, t);
    }

    public TraversalInterruptedException(final Traversal interruptedTraversal) {
        this(interruptedTraversal, null);
    }

    public TraversalInterruptedException(final Traversal interruptedTraversal, final Throwable cause) {
        super(String.format("The %s thread received interruption notification while iterating %s - it did not complete",
                Thread.currentThread().getName(), null == interruptedTraversal ? "" : interruptedTraversal), cause);
        this.interruptedTraversal = Optional.ofNullable(interruptedTraversal);
    }

    public Optional<Traversal> getInterruptedTraversal() {
        return interruptedTraversal;
    }
}
