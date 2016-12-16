/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.process.actor.util;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.actor.GraphActors;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GraphActorsHelper {

    private GraphActorsHelper() {

    }

    public static GraphActors configure(GraphActors actors, final Configuration configuration) {
        final Iterator<String> keys = IteratorUtils.asList(configuration.getKeys()).iterator();
        while (keys.hasNext()) {
            final String key = keys.next();
            if (key.equals(GraphActors.GRAPH_ACTORS_WORKERS))
                actors = actors.workers(configuration.getInt(GraphActors.GRAPH_ACTORS_WORKERS));
            else if (!key.equals(GraphActors.GRAPH_ACTORS))
                actors = actors.configure(key, configuration.getProperty(key));
        }
        return actors;
    }
}
