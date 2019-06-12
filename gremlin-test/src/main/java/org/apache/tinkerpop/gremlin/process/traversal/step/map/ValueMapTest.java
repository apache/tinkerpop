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
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.WithOptions;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.CREW;
import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.apache.tinkerpop.gremlin.structure.T.id;
import static org.apache.tinkerpop.gremlin.structure.T.label;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class ValueMapTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Map<Object, List>> get_g_V_valueMap();

    public abstract Traversal<Vertex, Map<Object, Object>> get_g_V_valueMapXtrueX();

    public abstract Traversal<Vertex, Map<Object, Object>> get_g_V_valueMap_withXtokensX();

    public abstract Traversal<Vertex, Map<Object, List>> get_g_V_valueMapXname_ageX();

    public abstract Traversal<Vertex, Map<Object, Object>> get_g_V_hasLabelXpersonX_filterXoutEXcreatedXX_valueMap_withXtokensX();

    public abstract Traversal<Vertex, Map<Object, Object>> get_g_V_valueMapXtrue_name_ageX();

    public abstract Traversal<Vertex, Map<Object, Object>> get_g_V_valueMapXname_ageX_withXtokensX();

    public abstract Traversal<Vertex, Map<Object, Object>> get_g_V_valueMapXname_ageX_withXtokens_labelsX_byXunfoldX();

    public abstract Traversal<Vertex, Map<Object, List<String>>> get_g_VX1X_outXcreatedX_valueMap(final Object v1Id);

    public abstract Traversal<Vertex, Map<Object, Object>> get_g_VX1X_valueMapXname_locationX_byXunfoldX_by(final Object v1Id);

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_valueMap() {
        final Traversal<Vertex, Map<Object, List>> traversal = get_g_V_valueMap();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Map<Object, List> values = traversal.next();
            final String name = (String) values.get("name").get(0);
            assertEquals(2, values.size());
            if (name.equals("marko")) {
                assertEquals(29, values.get("age").get(0));
            } else if (name.equals("josh")) {
                assertEquals(32, values.get("age").get(0));
            } else if (name.equals("peter")) {
                assertEquals(35, values.get("age").get(0));
            } else if (name.equals("vadas")) {
                assertEquals(27, values.get("age").get(0));
            } else if (name.equals("lop")) {
                assertEquals("java", values.get("lang").get(0));
            } else if (name.equals("ripple")) {
                assertEquals("java", values.get("lang").get(0));
            } else {
                throw new IllegalStateException("It is not possible to reach here: " + values);
            }
        }
        assertEquals(6, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_valueMap_withXtokensX() {
        for (final Traversal<Vertex, Map<Object, Object>> traversal :
                Arrays.asList(get_g_V_valueMapXtrueX(), get_g_V_valueMap_withXtokensX())) {
            printTraversalForm(traversal);
            int counter = 0;
            while (traversal.hasNext()) {
                counter++;
                final Map<Object, Object> values = traversal.next();
                final String name = (String) ((List) values.get("name")).get(0);
                assertEquals(4, values.size());
                assertThat(values.containsKey(T.id), is(true));
                if (name.equals("marko")) {
                    assertEquals(29, ((List) values.get("age")).get(0));
                    assertEquals("person", values.get(T.label));
                } else if (name.equals("josh")) {
                    assertEquals(32, ((List) values.get("age")).get(0));
                    assertEquals("person", values.get(T.label));
                } else if (name.equals("peter")) {
                    assertEquals(35, ((List) values.get("age")).get(0));
                    assertEquals("person", values.get(T.label));
                } else if (name.equals("vadas")) {
                    assertEquals(27, ((List) values.get("age")).get(0));
                    assertEquals("person", values.get(T.label));
                } else if (name.equals("lop")) {
                    assertEquals("java", ((List) values.get("lang")).get(0));
                    assertEquals("software", values.get(T.label));
                } else if (name.equals("ripple")) {
                    assertEquals("java", ((List) values.get("lang")).get(0));
                    assertEquals("software", values.get(T.label));
                } else {
                    throw new IllegalStateException("It is not possible to reach here: " + values);
                }
            }
            assertEquals(6, counter);
        }
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_valueMapXname_ageX() {
        final Traversal<Vertex, Map<Object, List>> traversal = get_g_V_valueMapXname_ageX();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Map<Object, List> values = traversal.next();
            final String name = (String) values.get("name").get(0);
            if (name.equals("marko")) {
                assertEquals(29, values.get("age").get(0));
                assertEquals(2, values.size());
            } else if (name.equals("josh")) {
                assertEquals(32, values.get("age").get(0));
                assertEquals(2, values.size());
            } else if (name.equals("peter")) {
                assertEquals(35, values.get("age").get(0));
                assertEquals(2, values.size());
            } else if (name.equals("vadas")) {
                assertEquals(27, values.get("age").get(0));
                assertEquals(2, values.size());
            } else if (name.equals("lop")) {
                assertNull(values.get("lang"));
                assertEquals(1, values.size());
            } else if (name.equals("ripple")) {
                assertNull(values.get("lang"));
                assertEquals(1, values.size());
            } else {
                throw new IllegalStateException("It is not possible to reach here: " + values);
            }
        }
        assertEquals(6, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_valueMapXname_ageX_withXtokensX() {
        for (final Traversal<Vertex, Map<Object, Object>> traversal :
                Arrays.asList(get_g_V_valueMapXtrue_name_ageX(), get_g_V_valueMapXname_ageX_withXtokensX())) {
            printTraversalForm(traversal);
            int counter = 0;
            while (traversal.hasNext()) {
                counter++;
                final Map<Object, Object> values = traversal.next();
                final String name = (String) ((List) values.get("name")).get(0);
                assertThat(values.containsKey(T.id), is(true));
                if (name.equals("marko")) {
                    assertEquals(4, values.size());
                    assertEquals(29, ((List) values.get("age")).get(0));
                    assertEquals("person", values.get(T.label));
                } else if (name.equals("josh")) {
                    assertEquals(4, values.size());
                    assertEquals(32, ((List) values.get("age")).get(0));
                    assertEquals("person", values.get(T.label));
                } else if (name.equals("peter")) {
                    assertEquals(4, values.size());
                    assertEquals(35, ((List) values.get("age")).get(0));
                    assertEquals("person", values.get(T.label));
                } else if (name.equals("vadas")) {
                    assertEquals(4, values.size());
                    assertEquals(27, ((List) values.get("age")).get(0));
                    assertEquals("person", values.get(T.label));
                } else if (name.equals("lop")) {
                    assertEquals(3, values.size());
                    assertNull(values.get("lang"));
                    assertEquals("software", values.get(T.label));
                } else if (name.equals("ripple")) {
                    assertEquals(3, values.size());
                    assertNull(values.get("lang"));
                    assertEquals("software", values.get(T.label));
                } else {
                    throw new IllegalStateException("It is not possible to reach here: " + values);
                }
            }
            assertEquals(6, counter);
        }
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_valueMapXname_ageX_withXtokens_labelsX_byXunfoldX() {
        final Traversal<Vertex, Map<Object, Object>> traversal = get_g_V_valueMapXname_ageX_withXtokens_labelsX_byXunfoldX();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Map<Object, Object> values = traversal.next();
            final String name = (String) values.get("name");
            assertThat(values.containsKey(T.id), is(false));
            if (name.equals("marko")) {
                assertEquals(3, values.size());
                assertEquals(29, values.get("age"));
                assertEquals("person", values.get(T.label));
            } else if (name.equals("josh")) {
                assertEquals(3, values.size());
                assertEquals(32, values.get("age"));
                assertEquals("person", values.get(T.label));
            } else if (name.equals("peter")) {
                assertEquals(3, values.size());
                assertEquals(35, values.get("age"));
                assertEquals("person", values.get(T.label));
            } else if (name.equals("vadas")) {
                assertEquals(3, values.size());
                assertEquals(27, values.get("age"));
                assertEquals("person", values.get(T.label));
            } else if (name.equals("lop")) {
                assertEquals(2, values.size());
                assertNull(values.get("lang"));
                assertEquals("software", values.get(T.label));
            } else if (name.equals("ripple")) {
                assertEquals(2, values.size());
                assertNull(values.get("lang"));
                assertEquals("software", values.get(T.label));
            } else {
                throw new IllegalStateException("It is not possible to reach here: " + values);
            }
        }
        assertEquals(6, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_outXcreatedX_valueMap() {
        final Traversal<Vertex, Map<Object, List<String>>> traversal = get_g_VX1X_outXcreatedX_valueMap(convertToVertexId("marko"));
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        final Map<Object, List<String>> values = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals("lop", values.get("name").get(0));
        assertEquals("java", values.get("lang").get(0));
        assertEquals(2, values.size());
    }

    /**
     * TINKERPOP-1483
     */
    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasLabelXpersonX_filterXoutEXcreatedXX_valueMap_withXtokensX() {
    	final Traversal<Vertex,Map<Object,Object>> gt = get_g_V_hasLabelXpersonX_filterXoutEXcreatedXX_valueMap_withXtokensX();
    	int cnt = 0;
    	while(gt.hasNext()){
    		final Map<Object,Object> m = gt.next();
    		assertTrue(m.size() > 0);
    		for (Object o : m.keySet()){
    			assertNotNull(m.get(o));
    		}
    		assertTrue(m.containsKey(id));
    		assertTrue(m.containsKey(label));
    		assertEquals("person",m.get(label));
    		cnt++;
    	}
    	// check we had results
    	assertTrue(cnt > 0);
    }

    @Test
    @LoadGraphWith(CREW)
    public void g_VX1X_valueMapXname_locationX_byXunfoldX_by() {
        final Traversal<Vertex,Map<Object,Object>> traversal = get_g_VX1X_valueMapXname_locationX_byXunfoldX_by(convertToVertexId("marko"));
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        final Map<Object, Object> values = traversal.next();
        assertFalse(traversal.hasNext());
        assertTrue("name value should be a String", values.containsKey("name") && String.class.equals(values.get("name").getClass()));
        assertTrue("location value should be a List", values.containsKey("location") && List.class.isAssignableFrom(values.get("location").getClass()));
        assertEquals("marko", values.get("name"));
        final List locations = (List) values.get("location");
        for (final String location : Arrays.asList("san diego", "santa cruz", "brussels", "santa fe")) {
            assertTrue(locations.contains(location));
        }
        assertEquals(2, values.size());
    }

    public static class Traversals extends ValueMapTest {
        @Override
        public Traversal<Vertex, Map<Object, List>> get_g_V_valueMap() {
            return g.V().valueMap();
        }

        @Override
        public Traversal<Vertex, Map<Object, Object>> get_g_V_valueMapXtrueX() {
            return g.V().valueMap(true);
        }

        @Override
        public Traversal<Vertex, Map<Object, Object>> get_g_V_valueMap_withXtokensX() {
            return g.V().valueMap().with(WithOptions.tokens);
        }

        @Override
        public Traversal<Vertex, Map<Object, List>> get_g_V_valueMapXname_ageX() {
            return g.V().valueMap("name", "age");
        }

        @Override
        public Traversal<Vertex, Map<Object, Object>> get_g_V_valueMapXtrue_name_ageX() {
            return g.V().valueMap(true, "name", "age");
        }

        @Override
        public Traversal<Vertex, Map<Object, Object>> get_g_V_valueMapXname_ageX_withXtokensX() {
            return g.V().valueMap("name", "age").with(WithOptions.tokens);
        }

        @Override
        public Traversal<Vertex, Map<Object, Object>> get_g_V_valueMapXname_ageX_withXtokens_labelsX_byXunfoldX() {
            return g.V().valueMap("name", "age").with(WithOptions.tokens, WithOptions.labels).by(__.unfold());
        }

        @Override
        public Traversal<Vertex, Map<Object, List<String>>> get_g_VX1X_outXcreatedX_valueMap(final Object v1Id) {
            return g.V(v1Id).out("created").valueMap();
        }

        @Override
        public Traversal<Vertex, Map<Object, Object>> get_g_V_hasLabelXpersonX_filterXoutEXcreatedXX_valueMap_withXtokensX() {
        	return g.V().hasLabel("person").filter(__.outE("created")).valueMap().with(WithOptions.tokens);
        }

        @Override
        public Traversal<Vertex, Map<Object, Object>> get_g_VX1X_valueMapXname_locationX_byXunfoldX_by(final Object v1Id) {
            return g.V(v1Id).valueMap("name","location").by(__.unfold()).by();
        }

    }
}
