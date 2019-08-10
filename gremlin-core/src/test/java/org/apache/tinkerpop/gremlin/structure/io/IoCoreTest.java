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
package org.apache.tinkerpop.gremlin.structure.io;

import org.apache.tinkerpop.gremlin.CoreTestHelper;
import org.apache.tinkerpop.gremlin.structure.io.graphml.GraphMLIo;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class IoCoreTest {
    @Test
    public void shouldBeUtilityClass() throws Exception {
        CoreTestHelper.assertIsUtilityClass(IoCore.class);
    }

    @Test
    public void shouldCreateAnIoBuilderforGraphML() {
        assertThat(IoCore.graphml(), instanceOf(GraphMLIo.Builder.class));
    }

    @Test
    public void shouldCreateAnIoBuilderforGraphSON() {
        assertThat(IoCore.graphson(), instanceOf(GraphSONIo.Builder.class));
    }

    @Test
    public void shouldCreateAnIoBuilderforGryo() {
        assertThat(IoCore.gryo(), instanceOf(GryoIo.Builder.class));
    }
}
