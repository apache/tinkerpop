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

package org.apache.tinkerpop.gremlin.hadoop.groovy.plugin;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.groovy.loaders.GremlinLoader;
import org.apache.tinkerpop.gremlin.groovy.plugin.GremlinPlugin;
import org.apache.tinkerpop.gremlin.groovy.plugin.PluginAcceptor;
import org.apache.tinkerpop.gremlin.groovy.plugin.RemoteAcceptor;
import org.apache.tinkerpop.gremlin.groovy.util.TestableConsolePluginAcceptor;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.HadoopGremlinSuite;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.util.Gremlin;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.codehaus.groovy.tools.shell.Groovysh;
import org.codehaus.groovy.tools.shell.IO;
import org.junit.Before;
import org.junit.Test;

import javax.script.ScriptException;
import javax.tools.JavaCompiler;
import javax.tools.SimpleJavaFileObject;
import javax.tools.ToolProvider;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This is an test that is mean to be used in the context of the {@link HadoopGremlinSuite} and shouldn't be
 * executed on its own.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HadoopGremlinPluginCheck extends AbstractGremlinTest {
    // ***********************
    // This test will be removed as the old "plugin" infrastructure was deprecated in 3.2.4. need to rework the
    // tests a bi for this to see what the model is for validating the jsr223 plugins. note that the code from
    // gremlin-groovy-test, specifically TestableConsolePluginAcceptor, has been copied to the bottom of this
    // file for reference
    // ***********************


    /**
    @Before
    public void setupTest() {
        try {
            this.console = new TestableConsolePluginAcceptor();
            final HadoopGremlinPlugin plugin = new HadoopGremlinPlugin();
            plugin.pluginTo(this.console);
            this.remote = (HadoopRemoteAcceptor) plugin.remoteAcceptor().get();
        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    ///////////////////

    private HadoopRemoteAcceptor remote;
    private TestableConsolePluginAcceptor console;

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldSupportRemoteTraversal() throws Exception {
        this.console.addBinding("graph", this.graph);
        this.console.addBinding("g", this.g);
        this.remote.connect(Arrays.asList("graph", "g"));
        //
        Traversal<?, ?> traversal = (Traversal<?, ?>) this.remote.submit(Arrays.asList("g.V().count()"));
        assertEquals(6L, traversal.next());
        assertFalse(traversal.hasNext());
        assertNotNull(this.console.getBindings().get(RemoteAcceptor.RESULT));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldSupportRemoteSugarTraversal() throws Exception {
        SugarTestHelper.clearRegistry(this.graphProvider);
        this.console.addBinding("graph", this.graph);
        this.console.addBinding("g", this.g);
        //
        this.remote.connect(Arrays.asList("graph", "g"));
        try {
            this.remote.submit(Arrays.asList("g.V.name.map{it.length()}.sum"));
            fail("Should not allow sugar usage");
        } catch (final Exception e) {
            // this is good
        }
        //
        this.remote.configure(Arrays.asList("useSugar", "true"));
        this.remote.connect(Arrays.asList("graph", "g"));
        Traversal<?, ?> traversal = (Traversal<?, ?>) this.remote.submit(Arrays.asList("g.V.name.map{it.length()}.sum"));
        assertEquals(28l, traversal.next());
        assertFalse(traversal.hasNext());
        assertNotNull(this.console.getBindings().get(RemoteAcceptor.RESULT));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldSupportRemoteGroupTraversal() throws Exception {
        SugarTestHelper.clearRegistry(this.graphProvider);
        GremlinLoader.load();
        this.console.addBinding("graph", this.graph);
        this.console.addBinding("g", this.g);
        this.remote.connect(Arrays.asList("graph"));
        //
        this.remote.connect(Arrays.asList("graph", "g"));
        Traversal<?, Map<String, List<String>>> traversal = (Traversal<?, Map<String, List<String>>>) this.remote.submit(Arrays.asList("g.V().out().group().by{it.value('name')[1]}.by('name')"));
        Map<String, List<String>> map = traversal.next();
        assertEquals(3, map.size());
        assertEquals(1, map.get("a").size());
        assertEquals("vadas", map.get("a").get(0));
        assertEquals(1, map.get("i").size());
        assertEquals("ripple", map.get("i").get(0));
        assertEquals(4, map.get("o").size());
        assertTrue(map.get("o").contains("josh"));
        assertTrue(map.get("o").contains("lop"));
        assertNotNull(this.console.getBindings().get(RemoteAcceptor.RESULT));
        //
        traversal = (Traversal<?, Map<String, List<String>>>) this.remote.submit(Arrays.asList("g.V().out().group().by(label).by{it.value('name')[1]}"));
        map = traversal.next();
        assertEquals(2, map.size());
        assertEquals(4, map.get("software").size());
        assertTrue(map.get("software").contains("o"));
        assertTrue(map.get("software").contains("i"));
        assertEquals(2, map.get("person").size());
        assertTrue(map.get("person").contains("o"));
        assertTrue(map.get("person").contains("a"));
        assertNotNull(this.console.getBindings().get(RemoteAcceptor.RESULT));
    }


    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldSupportHDFSMethods() throws Exception {
        List<String> ls = (List<String>) this.console.eval("hdfs.ls()");
        for (final String line : ls) {
            assertTrue(line.startsWith("-") || line.startsWith("r") || line.startsWith("w") || line.startsWith("x"));
            assertEquals(" ", line.substring(9, 10));
        }
        ls = (List<String>) this.console.eval("fs.ls()");
        for (final String line : ls) {
            assertTrue(line.startsWith("-") || line.startsWith("r") || line.startsWith("w") || line.startsWith("x"));
            assertEquals(" ", line.substring(9, 10));
        }
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldGracefullyHandleBadGremlinHadoopLibs() throws Exception {
        System.setProperty(Constants.HADOOP_GREMLIN_LIBS, TestHelper.makeTestDataDirectory(HadoopGremlinPluginCheck.class, "shouldGracefullyHandleBadGremlinHadoopLibs"));
        this.graph.configuration().setProperty(Constants.GREMLIN_HADOOP_JARS_IN_DISTRIBUTED_CACHE, true);
        this.console.addBinding("graph", this.graph);
        this.console.addBinding("g", this.g);
        this.remote.connect(Arrays.asList("graph", "g"));
        Traversal<?, ?> traversal = (Traversal<?, ?>) this.remote.submit(Arrays.asList("g.V()"));
        assertEquals(6, IteratorUtils.count(traversal));
        assertNotNull(this.console.getBindings().get(RemoteAcceptor.RESULT));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldSupportVariousFileSystemsInGremlinHadoopLibs() throws Exception {

        // The whole point of this test is to verify that HADOOP_GREMLIN_LIBS may contain paths with or without
        // a file system scheme prefix and that either path is properly handled. If all jar files, that were specified
        // in HADOOP_GREMLIN_LIBS, are found in the GraphComputers temporary directory after using the GraphComputer
        // (by submitting a traversal), the test is considered to be successful.
        //
        // The traversal will likely never fail, since both - Spark and Giraph - run in the same JVM during tests. This
        // is unfortunate as it doesn't allow us to verify that GraphComputers load jars properly in a distributed
        // environment. The test would fail in a distributed environment, IF loading the jars specified in
        // HADOOP_GREMLIN_LIBS wouldn't work. That is because we generate new jar files on the fly that are definitely
        // not part of any existing directory or the current classpath.

        final String testDataDirectory = TestHelper.makeTestDataDirectory(HadoopGremlinPluginCheck.class, "shouldHandleLocalGremlinHadoopLibs");
        final File jarFile1 = createJarFile(testDataDirectory + File.separator + "1", "Greeter1");
        final File jarFile2 = createJarFile(testDataDirectory + File.separator + "2", "Greeter2");
        final String graphComputerJarTargetBasePath = System.getProperty("java.io.tmpdir") + File.separator +
                "hadoop-gremlin-" + Gremlin.version() + "-libs" + File.separator;
        final File graphComputerJarTargetPath1 = new File(graphComputerJarTargetBasePath + "1" + File.separator + "Greeter1.jar");
        final File graphComputerJarTargetPath2 = new File(graphComputerJarTargetBasePath + "2" + File.separator + "Greeter2.jar");

        for (final boolean withScheme : Arrays.asList(false, true)) {

            Stream<String> hadoopGremlinLibs = Arrays.asList(jarFile1, jarFile2).stream().map(f -> f.getParentFile().getAbsolutePath());
            if (withScheme) {
                hadoopGremlinLibs = hadoopGremlinLibs.map(path -> "file://" + path);
            }
            System.setProperty(Constants.HADOOP_GREMLIN_LIBS, String.join(File.pathSeparator, hadoopGremlinLibs.collect(Collectors.toList())));

            this.graph.configuration().setProperty(Constants.GREMLIN_HADOOP_JARS_IN_DISTRIBUTED_CACHE, true);
            this.console.addBinding("graph", this.graph);
            this.console.addBinding("g", this.g);
            this.remote.connect(Arrays.asList("graph", "g"));

            Traversal<?, ?> traversal = (Traversal<?, ?>) this.remote.submit(Arrays.asList(
                    "ClassLoader.getSystemClassLoader().addURL('" + jarFile1.toURI().toURL() + "'.toURL());",
                    "ClassLoader.getSystemClassLoader().addURL('" + jarFile2.toURI().toURL() + "'.toURL());",
                    "g.V().choose(hasLabel('person'), " +
                            "values('name').map {Class.forName('Greeter1').hello(it.get())}, " +
                            "values('name').map {Class.forName('Greeter2').hello(it.get())})"));

            final List<String> expectedMessages = Arrays.asList("marko", "josh", "peter", "vadas").stream().
                    map(name -> "Greeter1 says: Hello " + name + "!").collect(Collectors.toList());

            expectedMessages.addAll(Arrays.asList("lop", "ripple").stream().
                    map(name -> "Greeter2 says: Hello " + name + "!").collect(Collectors.toList()));

            while (traversal.hasNext()) {
                final String message = (String) traversal.next();
                assertTrue(expectedMessages.remove(message));
            }

            assertEquals(0, expectedMessages.size());
        }

        assertTrue(graphComputerJarTargetPath1.exists());
        assertTrue(graphComputerJarTargetPath2.exists());

        assert graphComputerJarTargetPath1.delete();
        assert graphComputerJarTargetPath2.delete();
    }

    private File createJarFile(final String directory, final String className) throws IOException {

        new File(directory).mkdirs();

        final File classFile = new File(directory + File.separator + className + ".class");
        final File jarFile = new File(directory + File.separator + className + ".jar");

        jarFile.deleteOnExit();

        final JavaStringObject source = new JavaStringObject(className,
                "public class " + className + " {\n" +
                        "    public static String hello(final String name) {\n" +
                        "        return \"" + className + " says: Hello \" + name + \"!\";\n" +
                        "    }\n" +
                        "}");

        final JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        final List<String> options = Arrays.asList(
                "-d", classFile.getParentFile().getAbsolutePath()
        );
        assert compiler.getTask(null, null, null, options, null, Collections.singletonList(source)).call();

        final Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");

        try (final JarOutputStream target = new JarOutputStream(new FileOutputStream(jarFile), manifest)) {
            final JarEntry entry = new JarEntry(classFile.getName());
            entry.setTime(classFile.lastModified());
            target.putNextEntry(entry);
            try (final FileInputStream fis = new FileInputStream(classFile);
                 final BufferedInputStream in = new BufferedInputStream(fis)) {
                final byte buffer[] = new byte[1024];
                while (true) {
                    final int count = in.read(buffer);
                    if (count < 0) break;
                    target.write(buffer, 0, count);
                }
            }
            target.closeEntry();
        }

        assert classFile.delete();

        return jarFile;
    }

    private static class JavaStringObject extends SimpleJavaFileObject {

        private final String code;

        JavaStringObject(final String className, final String code) {
            super(URI.create("string:///" + className.replace(".", "/") + Kind.SOURCE.extension), Kind.SOURCE);
            this.code = code;
        }

        @Override
        public CharSequence getCharContent(boolean ignoreEncodingErrors) throws IOException {
            return code;
        }
    }
        **/


    /////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////


//package org.apache.tinkerpop.gremlin.groovy.util;
//
//import org.apache.tinkerpop.gremlin.groovy.plugin.GremlinPlugin;
//import org.apache.tinkerpop.gremlin.groovy.plugin.PluginAcceptor;
//import org.codehaus.groovy.tools.shell.Groovysh;
//import org.codehaus.groovy.tools.shell.IO;
//
//import javax.script.ScriptException;
//import java.io.IOException;
//import java.io.OutputStream;
//import java.util.Collections;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.Set;
//
//    /**
//     * @author Marko A. Rodriguez (http://markorodriguez.com)
//     */
//    public class TestableConsolePluginAcceptor implements PluginAcceptor {
//
//        public static final String ENVIRONMENT_NAME = "console";
//        public static final String ENVIRONMENT_SHELL = "ConsolePluginAcceptor.shell";
//        public static final String ENVIRONMENT_IO = "ConsolePluginAcceptor.io";
//
//        private Groovysh shell = new Groovysh(new IO(System.in, new OutputStream() {
//            @Override
//            public void write(int b) throws IOException {
//
//            }
//        }, System.err));
//
//        @Override
//        public void addImports(final Set<String> importStatements) {
//            importStatements.forEach(this.shell::execute);
//        }
//
//        @Override
//        public void addBinding(final String key, final Object val) {
//            this.shell.getInterp().getContext().setVariable(key, val);
//        }
//
//        @Override
//        public Map<String, Object> getBindings() {
//            return Collections.unmodifiableMap(this.shell.getInterp().getContext().getVariables());
//        }
//
//        @Override
//        public Object eval(final String script) throws ScriptException {
//            return this.shell.execute(script);
//        }
//
//        @Override
//        public Map<String, Object> environment() {
//            final Map<String, Object> env = new HashMap<>();
//            env.put(GremlinPlugin.ENVIRONMENT, ENVIRONMENT_NAME);
//            env.put(ENVIRONMENT_IO, this.shell.getIo());
//            env.put(ENVIRONMENT_SHELL, this.shell);
//            return env;
//        }
//
//    }
}
