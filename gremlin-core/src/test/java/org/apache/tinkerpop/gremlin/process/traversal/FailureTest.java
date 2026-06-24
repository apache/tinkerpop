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
package org.apache.tinkerpop.gremlin.process.traversal;

import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertNotNull;

/**
 * Tests for the default methods on the {@link Failure} interface, with particular focus on {@link Failure#format()}
 * gracefully handling {@code Failure} implementations that do not carry traversal/traverser context (e.g. those
 * constructed from a remote response such as {@code FailResponseException}).
 */
public class FailureTest {

    /**
     * Minimal {@link Failure} that returns {@code null} for the traversal and traverser context the same way a
     * {@code Failure} reconstructed from a remote server response would.
     */
    private static class RemoteLikeFailure implements Failure {
        private final String message;
        private final Map<String, Object> metadata;

        private RemoteLikeFailure(final String message, final Map<String, Object> metadata) {
            this.message = message;
            this.metadata = metadata;
        }

        @Override
        public String getMessage() {
            return message;
        }

        @Override
        public Map<String, Object> getMetadata() {
            return metadata;
        }

        @Override
        public Traverser.Admin getTraverser() {
            return null;
        }

        @Override
        public Traversal.Admin getTraversal() {
            return null;
        }
    }

    @Test
    public void shouldFormatWithoutTraversalAndTraverserContext() {
        final Failure failure = new RemoteLikeFailure("make it stop", Collections.emptyMap());

        // would throw a NullPointerException prior to the null guards being added to format()
        final String formatted = failure.format();

        assertNotNull(formatted);
        assertThat(formatted, containsString("fail() Step Triggered"));
        assertThat(formatted, containsString("Message  > make it stop"));
        assertThat(formatted, containsString("Metadata > {}"));

        // these sections depend on traversal/traverser context which is absent so they should be omitted entirely
        assertThat(formatted, not(containsString("Traverser>")));
        assertThat(formatted, not(containsString("Traversal>")));
        assertThat(formatted, not(containsString("Parent   >")));
    }

    @Test
    public void shouldFormatWithNullMessageAndMetadata() {
        final Failure failure = new RemoteLikeFailure(null, null);

        // null message and metadata should still format without error
        final String formatted = failure.format();

        assertNotNull(formatted);
        assertThat(formatted, containsString("Message  > null"));
        assertThat(formatted, containsString("Metadata > null"));
    }
}
