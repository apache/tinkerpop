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
package org.apache.tinkerpop.gremlin.groovy.plugin;


import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;


/**
 * @author Nghia Tran (https://github.com/n-tran)
 */
public class ArtifactTest {
    @Test
    public void shouldBeEqualIfSame() {
        final Artifact a = new Artifact("org.apache.tinkerpop","tinkergraph-gremlin","3.0.0");
        final Artifact b = a;

        assertThat(a.equals(b), is(true));
    }

    @Test
    public void shouldNotBeEqualIfArgumentIsNull() {
        final Artifact a = new Artifact("org.apache.tinkerpop","tinkergraph-gremlin","3.0.0");
        final Artifact b = null;

        assertThat(a.equals(b), is(false));
    }

    @Test
    public void shouldNotBeEqualIfArgumentIsNotAnArtifact() {
        final Artifact a = new Artifact("org.apache.tinkerpop","tinkergraph-gremlin","3.0.0");
        final String b = " ";

        assertThat(a.equals(b), is(false));
    }

    @Test
    public void shouldNotBeEqualIfTheGroupIsNotEqual() {
        final Artifact a = new Artifact("org.apache.tinkerpop","tinkergraph-gremlin","3.0.0");
        final Artifact b = new Artifact("com.apacheTest.tinkerpop2","tinkergraph-gremlin","3.0.0");

        assertThat(a.equals(b), is(false));
    }

    @Test
    public void shouldNotBeEqualIfTheArtifactIsNotEqual() {
        final Artifact a = new Artifact("org.apache.tinkerpop","tinkergraph-gremlin","3.0.0");
        final Artifact b = new Artifact("org.apache.tinkerpop","tinkergraph-artifact","3.0.0");

        assertThat(a.equals(b), is(false));
    }

    @Test
    public void shouldNotBeEqualIfTheVersionIsNotEqual() {
        final Artifact a = new Artifact("org.apache.tinkerpop","tinkergraph-gremlin","3.0.0");
        final Artifact b = new Artifact("org.apache.tinkerpop","tinkergraph-gremlin","4.0.0");

        assertThat(a.equals(b), is(false));
    }

    @Test
    public void shouldBeEqual() {
        final Artifact a = new Artifact("org.apache.tinkerpop","tinkergraph-gremlin","3.0.0");
        final Artifact b = new Artifact("org.apache.tinkerpop","tinkergraph-gremlin","3.0.0");

        assertThat(a.equals(b), is(true));
    }
}
