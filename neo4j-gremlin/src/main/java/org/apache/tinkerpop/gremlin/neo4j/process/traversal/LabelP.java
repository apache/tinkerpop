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
package org.apache.tinkerpop.gremlin.neo4j.process.traversal;

import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jVertex;
import org.apache.tinkerpop.gremlin.process.traversal.P;

import java.io.Serializable;
import java.util.function.BiPredicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class LabelP extends P<String> {

    private LabelP(final String label) {
        super(LabelBiPredicate.instance(), label);
    }

    public static P<String> of(final String label) {
        return new LabelP(label);
    }

    public static final class LabelBiPredicate implements BiPredicate<String, String>, Serializable {

        private static final LabelBiPredicate INSTANCE = new LabelBiPredicate();

        private LabelBiPredicate() {
        }

        @Override
        public boolean test(final String labels, final String checkLabel) {
            return labels.equals(checkLabel) || labels.contains(Neo4jVertex.LABEL_DELIMINATOR + checkLabel) || labels.contains(checkLabel + Neo4jVertex.LABEL_DELIMINATOR);
        }

        public static LabelBiPredicate instance() {
            return INSTANCE;
        }
    }

}
