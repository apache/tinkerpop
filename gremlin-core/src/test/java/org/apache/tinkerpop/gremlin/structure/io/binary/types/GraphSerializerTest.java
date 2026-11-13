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
package org.apache.tinkerpop.gremlin.structure.io.binary.types;

import org.apache.tinkerpop.gremlin.structure.util.DefaultGraphProvider;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * {@code gremlin-core} has no dependency on any concrete {@link org.apache.tinkerpop.gremlin.structure.Graph}
 * implementation, so no {@link DefaultGraphProvider} is ever on this module's class or module path. That
 * makes it the right place to exercise {@link GraphSerializer}'s "no provider" failure, with the improved message
 * naming both remedies. The companion "with a provider" case is covered in {@code gremlin-util}, which has
 * {@code tinkergraph-gremlin} as a test dependency.
 */
public class GraphSerializerTest {

    @Test
    public void shouldHaveNoDefaultGraphProviderInGremlinCore() {
        assertFalse("gremlin-core must not see a DefaultGraphProvider on its own test classpath - this test " +
                "belongs to whichever module pulled one in", DefaultGraphProvider.find().isPresent());
    }

    @Test
    public void shouldFailWithImprovedMessageWhenNoProviderIsRegistered() {
        // the provider lookup happens before buffer/context are touched, so null arguments are safe here
        try {
            new GraphSerializer().readValue(null, null);
            fail("expected an IOException naming both remedies for the missing DefaultGraphProvider");
        } catch (final IOException ex) {
            assertEquals(DefaultGraphProvider.NO_PROVIDER_MESSAGE, ex.getMessage());
        }
    }
}
