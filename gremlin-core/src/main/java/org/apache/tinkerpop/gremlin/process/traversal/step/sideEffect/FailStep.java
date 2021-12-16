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
package org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect;

import org.apache.tinkerpop.gremlin.process.traversal.Failure;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;

import java.util.Collections;
import java.util.Map;

/**
 * Triggers an immediate failure of the traversal by throwing a {@code RuntimeException}. The exception thrown must
 * implement the {@link Failure} interface.
 */
public class FailStep<S> extends SideEffectStep<S> {

    protected String message;
    protected Map<String,Object> metadata;

    public FailStep(final Traversal.Admin traversal) {
        this(traversal, "fail() step triggered");
    }

    public FailStep(final Traversal.Admin traversal, final String message) {
        this(traversal, message, Collections.emptyMap());
    }

    public FailStep(final Traversal.Admin traversal, final String message, final Map<String,Object> metadata) {
        super(traversal);
        this.message = message;
        this.metadata = metadata;
    }

    @Override
    protected void sideEffect(final Traverser.Admin<S> traverser) {
        throw new FailException(traversal, traverser, message, metadata);
    }

    /**
     * Default {@link Failure} implementation that is thrown by {@link FailStep}.
     */
    public static class FailException extends RuntimeException implements Failure {
        private final Map<String,Object> metadata;
        private final Traversal.Admin traversal;
        private final Traverser.Admin traverser;

        public FailException(final Traversal.Admin traversal, final Traverser.Admin traverser,
                             final String message, final Map<String,Object> metadata) {
            super(message);
            this.metadata = metadata;
            this.traversal = traversal;
            this.traverser = traverser;
        }

        @Override
        public Map<String, Object> getMetadata() {
            return metadata;
        }

        @Override
        public Traverser.Admin getTraverser() {
            return traverser;
        }

        @Override
        public Traversal.Admin getTraversal() {
            return traversal;
        }
    }
}
