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

import org.apache.commons.io.FileUtils;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.TestSupport;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.ser.SerTokens;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeThat;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractCompatibilityTest {
    protected static final File root = TestSupport.getRootOfBuildDirectory(Model.class);
    protected final Model model = Model.instance();

    public static void resetDirectory(final String pathToDelete) {
        final File f = new File(pathToDelete);
        if (f.exists()) {
            try {
                FileUtils.deleteDirectory(f);
            } catch (IOException ioe) {
                throw new RuntimeException(String.format(
                        "Manually delete the %s directory - it could not be done automatically", pathToDelete), ioe);
            }
        }
        f.mkdirs();
    }

    public abstract <T> T read(final byte[] bytes, final Class<T> clazz) throws Exception;

    public abstract byte[] write(final Object o, final Class<?> clazz, final String entryName) throws Exception;

    protected abstract byte[] readFromResource(final String resource) throws IOException;

    protected abstract String getCompatibility();

    protected <T> T findModelEntryObject(final String resourceName) {
        return model.find(resourceName).orElseThrow(() -> new IllegalStateException("Could not find requested model entry")).getObject();
    }

    protected void assertVertex(final Vertex expected, final Vertex actual) {
        assertEquals(expected.id(), actual.id());
        assertEquals(expected.label(), actual.label());

        assertEquals(IteratorUtils.count(expected.properties()), IteratorUtils.count(actual.properties()));
        for (String k : expected.keys()) {
            final Iterator<VertexProperty<Object>> expectedVps = expected.properties(k);
            final List<VertexProperty<Object>> actualVps = IteratorUtils.list(actual.properties(k));
            while (expectedVps.hasNext()) {
                final VertexProperty expectedVp = expectedVps.next();
                final VertexProperty<Object> found = actualVps.stream()
                        .filter(vp -> vp.id().equals(expectedVp.id()))
                        .findFirst()
                        .orElseThrow(() -> new RuntimeException("Could not find VertexProperty for " + expectedVp.id()));
                assertVertexProperty(expectedVp, found);
            }
        }
    }

    protected void assertEdge(final Edge expected, final Edge actual) {
        assertEquals(expected.id(), actual.id());
        assertEquals(expected.label(), actual.label());
        assertEquals(expected.inVertex().id(), actual.inVertex().id());
        assertEquals(expected.outVertex().id(), actual.outVertex().id());
        assertEquals(expected.inVertex().label(), actual.inVertex().label());
        assertEquals(expected.outVertex().label(), actual.outVertex().label());
        assertEquals(IteratorUtils.count(expected.properties()), IteratorUtils.count(actual.properties()));
        final Iterator<Property<Object>> itty = expected.properties();
        while(itty.hasNext()) {
            final Property p = itty.next();
            assertProperty(p, actual.property(p.key()));
        }
    }

    protected void assertVertexProperty(final VertexProperty expected, final VertexProperty actual) {
        assertEquals(expected.id(), actual.id());
        assertEquals(expected.label(), actual.label());
        assertEquals(IteratorUtils.count(expected.properties()), IteratorUtils.count(actual.properties()));
        final Iterator<Property> itty = expected.properties();
        while (itty.hasNext()) {
            final Property p = itty.next();
            assertProperty(p, actual.property(p.key()));
        }
    }

    protected void assertProperty(final Property expected, final Property actual) {
        assertEquals(expected.key(), actual.key());
        assertEquals(expected.value(), actual.value());
    }

    protected static void assertResponseMessage(final ResponseMessage resource, final ResponseMessage fromStatic,
                                                final ResponseMessage recycled) {
        assertEquals(resource.getStatus().getCode().code(), recycled.getStatus().getCode().code());
        assertEquals(resource.getStatus().getMessage(), recycled.getStatus().getMessage());
        assertEquals(resource.getStatus().getException(), recycled.getStatus().getException());
        assertEquals(resource.getResult().getData(), recycled.getResult().getData());

        assertEquals(resource.getStatus().getCode().code(), fromStatic.getStatus().getCode().code());
        assertEquals(resource.getStatus().getMessage(), fromStatic.getStatus().getMessage());
        assertEquals(resource.getStatus().getException(), fromStatic.getStatus().getException());
        assertEquals(resource.getResult().getData(), fromStatic.getResult().getData());
    }

    protected static void assertRequestMessage(final RequestMessage resource, final RequestMessage fromStatic,
                                               final RequestMessage recycled) {
        // Check per field rather than map equals since a new field may get added later.
        assertEquals(resource.getGremlin(), recycled.getGremlin());
        assertEquals(resource.<Map>getField(SerTokens.TOKEN_BINDINGS), recycled.getField(SerTokens.TOKEN_BINDINGS));
        assertEquals(resource.<String>getField(SerTokens.TOKEN_LANGUAGE), recycled.getField(SerTokens.TOKEN_LANGUAGE));
        assertEquals(resource.<String>getField(SerTokens.TOKEN_G), recycled.getField(SerTokens.TOKEN_G));
        assertEquals(resource.<Long>getField(SerTokens.TOKEN_TIMEOUT_MS), recycled.getField(SerTokens.TOKEN_TIMEOUT_MS));
        assertEquals(resource.<String>getField(SerTokens.TOKEN_MATERIALIZE_PROPERTIES), recycled.getField(SerTokens.TOKEN_MATERIALIZE_PROPERTIES));

        assertEquals(resource.getGremlin(), fromStatic.getGremlin());
        assertEquals(resource.<Map>getField(SerTokens.TOKEN_BINDINGS), fromStatic.getField(SerTokens.TOKEN_BINDINGS));
        assertEquals(resource.<String>getField(SerTokens.TOKEN_LANGUAGE), fromStatic.getField(SerTokens.TOKEN_LANGUAGE));
        assertEquals(resource.<String>getField(SerTokens.TOKEN_G), fromStatic.getField(SerTokens.TOKEN_G));
        assertEquals(resource.<Long>getField(SerTokens.TOKEN_TIMEOUT_MS), fromStatic.getField(SerTokens.TOKEN_TIMEOUT_MS));
        assertEquals(resource.<String>getField(SerTokens.TOKEN_MATERIALIZE_PROPERTIES), fromStatic.getField(SerTokens.TOKEN_MATERIALIZE_PROPERTIES));
    }
}
