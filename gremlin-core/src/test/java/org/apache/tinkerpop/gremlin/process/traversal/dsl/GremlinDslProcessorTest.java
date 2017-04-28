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
package org.apache.tinkerpop.gremlin.process.traversal.dsl;

import com.google.testing.compile.JavaFileObjects;
import org.junit.Test;

import javax.tools.StandardLocation;

import static com.google.common.truth.Truth.ASSERT;
import static com.google.testing.compile.JavaSourceSubjectFactory.javaSource;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinDslProcessorTest {

    @Test
    public void shouldCompileToDefaultPackage() {
        ASSERT.about(javaSource())
                .that(JavaFileObjects.forResource(GremlinDsl.class.getResource("SocialTraversalDsl.java")))
                .processedWith(new GremlinDslProcessor())
                .compilesWithoutError();
    }

    @Test
    public void shouldCompileAndMovePackage() {
        ASSERT.about(javaSource())
                .that(JavaFileObjects.forResource(GremlinDsl.class.getResource("SocialMoveTraversalDsl.java")))
                .processedWith(new GremlinDslProcessor())
                .compilesWithoutError()
                .and()
                .generatesFileNamed(StandardLocation.SOURCE_OUTPUT, "org.apache.tinkerpop.gremlin.process.traversal.dsl.social", "SocialMoveTraversal.java");
    }

    @Test
    public void shouldCompileTraversalAndTraversalSourceToDefaultPackage() {
        ASSERT.about(javaSource())
                .that(JavaFileObjects.forResource(GremlinDsl.class.getResource("SocialPackageTraversalDsl.java")))
                .processedWith(new GremlinDslProcessor())
                .compilesWithoutError();
    }
}
