/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.tinkerpop.gremlin.neo4j.structure.full;

import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jElement;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jProperty;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.neo4j.tinkerpop.api.Neo4jEntity;
import org.neo4j.tinkerpop.api.Neo4jNode;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
final class FullNeo4jProperty<V> extends Neo4jProperty {

    public FullNeo4jProperty(final Element element, final String key, final V value) {
        super(element, key, value);
    }

    @Override
    public void remove() {
        this.graph.tx().readWrite();
        if (this.element instanceof VertexProperty) {
            final Neo4jNode node = ((FullNeo4jVertexProperty) this.element).getBaseVertexProperty();
            if (null != node && node.hasProperty(this.key)) {
                node.removeProperty(this.key);
            }
        } else {
            final Neo4jEntity entity = ((Neo4jElement) this.element).getBaseElement();
            if (entity.hasProperty(this.key)) {
                entity.removeProperty(this.key);
            }
        }
    }
}

