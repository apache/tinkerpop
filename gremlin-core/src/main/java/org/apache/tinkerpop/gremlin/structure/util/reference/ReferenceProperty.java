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

package org.apache.tinkerpop.gremlin.structure.util.reference;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.Attachable;

import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ReferenceProperty<V> implements Attachable<Property<V>>, Serializable {

    private ReferenceElement<?> element;
    private String key;

    private ReferenceProperty() {

    }

    public ReferenceProperty(final Property<V> property) {
        this.element = ReferenceFactory.detach(property.element());
        this.key = property.key();
    }

    @Override
    public Property<V> attach(final Vertex hostVertex) throws IllegalStateException {
        return this.element.attach(hostVertex).property(this.key);
    }

    @Override
    public Property<V> attach(final Graph hostGraph) throws IllegalStateException {
        return this.element.attach(hostGraph).property(this.key);
    }

    @Override
    public int hashCode() {
        return this.element.hashCode() + this.key.hashCode();
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof ReferenceProperty && object.hashCode() == this.hashCode();
    }

    @Override
    public String toString() {
        return "p*[" + this.element.id + ":" + this.key + "]";
    }
}
