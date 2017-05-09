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
package ${package};

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SocialDslTest {

    private Graph graph = TinkerFactory.createModern();

    @Test
    public void shouldValidateThatMarkoKnowsJosh() {
        SocialTraversalSource social = graph.traversal(SocialTraversalSource.class);
        assertTrue(social.V().has("name","marko").knows("josh").hasNext());
        assertTrue(social.persons("marko").knows("josh").hasNext());
    }

    @Test
    public void shouldGetAgeOfYoungestFriendOfMarko() {
        SocialTraversalSource social = graph.traversal(SocialTraversalSource.class);
        assertEquals(27, social.V().has("name","marko").youngestFriendsAge().next().intValue());
        assertEquals(27, social.persons("marko").youngestFriendsAge().next().intValue());
    }

    @Test
    public void shouldFindAllPersons() {
        SocialTraversalSource social = graph.traversal(SocialTraversalSource.class);
        assertEquals(4, social.persons().count().next().intValue());
    }

    @Test
    public void shouldFindAllPersonsWithTwoOrMoreProjects() {
        SocialTraversalSource social = graph.traversal(SocialTraversalSource.class);
        assertEquals(1, social.persons().filter(__.createdAtLeast(2)).count().next().intValue());
    }
}