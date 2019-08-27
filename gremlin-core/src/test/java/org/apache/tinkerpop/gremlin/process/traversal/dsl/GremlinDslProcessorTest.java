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

import com.google.testing.compile.Compilation;
import com.google.testing.compile.JavaFileObjects;
import org.junit.Test;

import javax.tools.JavaFileObject;
import javax.tools.StandardLocation;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.testing.compile.CompilationSubject.assertThat;
import static com.google.testing.compile.Compiler.javac;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinDslProcessorTest {

    @Test
    public void shouldCompileToDefaultPackage() {
        final Compilation compilation = javac()
                .withProcessors(new GremlinDslProcessor())
                .compile(JavaFileObjects.forResource(GremlinDsl.class.getResource("SocialTraversalDsl.java")));
        assertThat(compilation).succeededWithoutWarnings();
    }

    @Test
    public void shouldCompileAndMovePackage() {
        final Compilation compilation = javac()
                .withProcessors(new GremlinDslProcessor())
                .compile(JavaFileObjects.forResource(GremlinDsl.class.getResource("SocialMoveTraversalDsl.java")));
        assertThat(compilation).succeededWithoutWarnings();
        assertThat(compilation)
                .generatedFile(StandardLocation.SOURCE_OUTPUT, "org.apache.tinkerpop.gremlin.process.traversal.dsl.social", "SocialMoveTraversal.java");
    }

    @Test
    public void shouldCompileTraversalAndTraversalSourceToDefaultPackage() {
        final Compilation compilation = javac()
                .withProcessors(new GremlinDslProcessor())
                .compile(JavaFileObjects.forResource(GremlinDsl.class.getResource("SocialPackageTraversalDsl.java")));
        assertThat(compilation).succeededWithoutWarnings();
    }

    @Test
    public void shouldCompileWithNoDefaultMethods() {
        final Compilation compilation = javac()
                .withProcessors(new GremlinDslProcessor())
                .compile(JavaFileObjects.forResource(GremlinDsl.class.getResource("SocialNoDefaultMethodsTraversalDsl.java")));
        assertThat(compilation).succeededWithoutWarnings();
    }

    @Test
    public void shouldCompileRemoteDslTraversal() {
        final Compilation compilation = javac()
                .withProcessors(new GremlinDslProcessor())
                .compile(JavaFileObjects.forResource(GremlinDsl.class.getResource("SocialTraversalDsl.java")),
                        JavaFileObjects.forResource(GremlinDsl.class.getResource("RemoteDslTraversal.java")));

        try {
            final ClassLoader cl = new JavaFileObjectClassLoader(compilation.generatedFiles());
            final Class cls = cl.loadClass("org.apache.tinkerpop.gremlin.process.traversal.dsl.RemoteDslTraversal");
            cls.getConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static class JavaFileObjectClassLoader extends ClassLoader {
        Map<String, JavaFileObject> classFileMap;

        JavaFileObjectClassLoader(List<JavaFileObject> classFiles) {
            classFileMap = classFiles.stream().collect(Collectors.toMap(
                    f -> f.toUri().toString()
                            .replaceFirst(".*(?=org/apache/tinkerpop)", ""),
                    Function.identity()));
        }

        public Class findClass(String name) {
            try {
                byte[] b = loadClassData(name);
                return defineClass(name, b, 0, b.length);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        private byte[] loadClassData(String name) throws IOException {
            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            final String classFilename = name.replaceAll("\\.", "/") + ".class";
            final InputStream in = classFileMap.get(classFilename).openInputStream();
            try {
                final byte[] buf = new byte[1024];
                int len = in.read(buf);
                while (len != -1) {
                    out.write(buf, 0, len);
                    len = in.read(buf);
                }
            } finally {
                in.close();
            }
            return out.toByteArray();
        }
    }
}
