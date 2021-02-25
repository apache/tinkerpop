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
package org.apache.tinkerpop.gremlin.structure.util.reference;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MutablePath;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ReferenceFactoryTest {

    @Test
    public void shouldDetachPathToReferenceWithEmbeddedLists() {
        final Path path = MutablePath.make();
        path.extend(DetachedVertex.build().setId(1).setLabel("person").
                addProperty(new DetachedVertexProperty<>(
                        101, "name", "stephen", Collections.emptyMap())).create(), Collections.singleton("a"));
        path.extend(Collections.singletonList(DetachedVertex.build().setId(2).setLabel("person").
                addProperty(new DetachedVertexProperty<>(
                        102, "name", "vadas", Collections.emptyMap())).create()), Collections.singleton("a"));
        path.extend(Collections.singletonList(Collections.singletonList(DetachedVertex.build().setId(3).setLabel("person").
                addProperty(new DetachedVertexProperty<>(
                        103, "name", "josh", Collections.emptyMap())).create())), Collections.singleton("a"));

        final Path detached = ReferenceFactory.detach(path);
        final Vertex v1  = detached.get(0);
        assertThat(v1, instanceOf(ReferenceVertex.class));
        assertThat(v1.properties().hasNext(), is(false));

        final Vertex v2  = (Vertex) ((List) detached.get(1)).get(0);
        assertThat(v2, instanceOf(ReferenceVertex.class));
        assertThat(v2.properties().hasNext(), is(false));

        final Vertex v3  = (Vertex) ((List) ((List) detached.get(2)).get(0)).get(0);
        assertThat(v3, instanceOf(ReferenceVertex.class));
        assertThat(v3.properties().hasNext(), is(false));
    }
}
