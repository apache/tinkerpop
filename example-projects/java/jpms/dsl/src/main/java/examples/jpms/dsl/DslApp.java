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
package examples.jpms.dsl;

import examples.jpms.dsl.social.SocialTraversalSource;
import examples.jpms.dsl.social.__;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

/**
 * Uses the traversal source generated from {@code SocialTraversalDsl} against an embedded TinkerGraph. Any failed
 * check throws, so the process exits with a non-zero status.
 */
public final class DslApp {

    private DslApp() {}

    public static void main(final String[] args) throws Exception {
        try (TinkerGraph graph = TinkerFactory.createModern()) {
            final SocialTraversalSource social = traversal(SocialTraversalSource.class).with(graph);

            check(List.of("josh"), social.V().has("name", "marko").knows("josh").values("name").toList(),
                    "knows() from V()");
            check(List.of("josh"), social.persons("marko").knows("josh").values("name").toList(),
                    "knows() from persons()");
            check(27, social.persons("marko").youngestFriendsAge().next(), "youngestFriendsAge()");
            check(List.of(2L), social.persons("josh").createdAtLeast(2).toList(), "createdAtLeast()");
            check(new HashSet<>(Arrays.asList("marko", "vadas", "josh", "peter")),
                    new HashSet<>(social.persons().values("name").toList()), "persons()");
            check(List.of("marko"), social.persons().where(__.knows("vadas")).values("name").toList(),
                    "anonymous knows()");
        }
        System.out.println("dsl example completed");
    }

    private static void check(final Object expected, final Object actual, final String what) {
        if (!expected.equals(actual))
            throw new IllegalStateException(what + ": expected " + expected + " but was " + actual);
    }
}
