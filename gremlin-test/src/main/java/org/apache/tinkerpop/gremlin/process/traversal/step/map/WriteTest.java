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

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.traversal.IO;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class WriteTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Object,Object> get_g_io_writeXkryoX(final String fileToWrite)  throws IOException;

    public abstract Traversal<Object,Object> get_g_io_write_withXwriter_gryoX(final String fileToWrite)  throws IOException;

    public abstract Traversal<Object,Object> get_g_io_writeXjsonX(final String fileToWrite)  throws IOException;

    public abstract Traversal<Object,Object> get_g_io_write_withXwriter_graphsonX(final String fileToWrite)  throws IOException;

    public abstract Traversal<Object,Object> get_g_io_writeXxmlX(final String fileToWrite)  throws IOException;

    public abstract Traversal<Object,Object> get_g_io_write_withXwriter_graphmlX(final String fileToWrite)  throws IOException;

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_writeXkryoX() throws IOException {
        final String fileToWrite = TestHelper.generateTempFile(WriteTest.class, "tinkerpop-modern-v3d0", ".kryo").getAbsolutePath().replace('\\', '/');

        final File f = new File(fileToWrite);
        assertThat(f.length() == 0, is(true));

        final Traversal<Object,Object> traversal = get_g_io_writeXkryoX(fileToWrite);
        printTraversalForm(traversal);
        assertFalse(traversal.hasNext());

        assertThat(f.length() > 0, is(true));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_io_write_withXwrite_gryoX() throws IOException {
        final String fileToWrite = TestHelper.generateTempFile(WriteTest.class, "tinkerpop-modern-v3d0", ".kryo").getAbsolutePath().replace('\\', '/');

        final File f = new File(fileToWrite);
        assertThat(f.length() == 0, is(true));

        final Traversal<Object,Object> traversal = get_g_io_write_withXwriter_gryoX(fileToWrite);
        printTraversalForm(traversal);
        assertFalse(traversal.hasNext());

        assertThat(f.length() > 0, is(true));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_writeXjsonX() throws IOException {
        final String fileToWrite = TestHelper.generateTempFile(WriteTest.class,"tinkerpop-modern-v3d0", ".json").getAbsolutePath().replace('\\', '/');

        final File f = new File(fileToWrite);
        assertThat(f.length() == 0, is(true));

        final Traversal<Object,Object> traversal = get_g_io_writeXjsonX(fileToWrite);
        printTraversalForm(traversal);
        assertFalse(traversal.hasNext());

        assertThat(f.length() > 0, is(true));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_io_write_withXwriter_graphsonX() throws IOException {
        final String fileToWrite = TestHelper.generateTempFile(WriteTest.class,"tinkerpop-modern-v3d0", ".json").getAbsolutePath().replace('\\', '/');

        final File f = new File(fileToWrite);
        assertThat(f.length() == 0, is(true));

        final Traversal<Object,Object> traversal = get_g_io_write_withXwriter_graphsonX(fileToWrite);
        printTraversalForm(traversal);
        assertFalse(traversal.hasNext());

        assertThat(f.length() > 0, is(true));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_writeXxmlX() throws IOException {
        final String fileToWrite = TestHelper.generateTempFile(WriteTest.class,"tinkerpop-modern", ".xml").getAbsolutePath().replace('\\', '/');

        final File f = new File(fileToWrite);
        assertThat(f.length() == 0, is(true));

        final Traversal<Object,Object> traversal = get_g_io_writeXxmlX(fileToWrite);
        printTraversalForm(traversal);
        assertFalse(traversal.hasNext());

        assertThat(f.length() > 0, is(true));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_io_write_withXwriter_graphmlX() throws IOException {
        final String fileToWrite = TestHelper.generateTempFile(WriteTest.class,"tinkerpop-modern", ".xml").getAbsolutePath().replace('\\', '/');

        final File f = new File(fileToWrite);
        assertThat(f.length() == 0, is(true));

        final Traversal<Object,Object> traversal = get_g_io_write_withXwriter_graphmlX(fileToWrite);
        printTraversalForm(traversal);
        assertFalse(traversal.hasNext());

        assertThat(f.length() > 0, is(true));
    }

    public static class Traversals extends WriteTest {
        @Override
        public Traversal<Object,Object> get_g_io_writeXkryoX(final String fileToWrite) throws IOException {
            return g.io(fileToWrite).write();
        }

        @Override
        public Traversal<Object,Object> get_g_io_write_withXwriter_gryoX(final String fileToWrite) throws IOException {
            return g.io(fileToWrite).with(IO.writer, IO.gryo).write();
        }

        @Override
        public Traversal<Object,Object> get_g_io_writeXjsonX(final String fileToWrite) throws IOException {
            return g.io(fileToWrite).write();
        }

        @Override
        public Traversal<Object,Object> get_g_io_write_withXwriter_graphsonX(final String fileToWrite) throws IOException {
            return g.io(fileToWrite).with(IO.writer, IO.graphson).write();
        }
        @Override
        public Traversal<Object,Object> get_g_io_writeXxmlX(final String fileToWrite) throws IOException {
            return g.io(fileToWrite).write();
        }

        @Override
        public Traversal<Object,Object> get_g_io_write_withXwriter_graphmlX(final String fileToWrite) throws IOException {
            return g.io(fileToWrite).with(IO.writer, IO.graphml).write();
        }
    }
}
