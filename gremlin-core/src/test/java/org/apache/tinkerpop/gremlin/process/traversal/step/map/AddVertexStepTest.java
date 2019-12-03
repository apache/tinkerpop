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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class AddVertexStepTest {

    @Test
    public void shouldDefaultTheLabelIfNullString() {
        final Traversal.Admin t = mock(Traversal.Admin.class);
        final AddVertexStartStep starStep = new AddVertexStartStep(t, (String) null);
        assertEquals(Vertex.DEFAULT_LABEL, starStep.getParameters().getRaw().get(T.label).get(0));
        final AddVertexStep step = new AddVertexStep(t, (String) null);
        assertEquals(Vertex.DEFAULT_LABEL, starStep.getParameters().getRaw().get(T.label).get(0));
    }

    @Test
    public void shouldDefaultTheLabelIfNullTraversal() {
        final Traversal.Admin t = mock(Traversal.Admin.class);
        final AddVertexStartStep starStep = new AddVertexStartStep(t, (Traversal<?,String>) null);
        assertEquals(Vertex.DEFAULT_LABEL, starStep.getParameters().getRaw().get(T.label).get(0));
        final AddVertexStep step = new AddVertexStep(t, (String) null);
        assertEquals(Vertex.DEFAULT_LABEL, starStep.getParameters().getRaw().get(T.label).get(0));
    }
}
