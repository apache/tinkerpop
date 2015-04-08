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

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.Attachable;

import java.io.Serializable;
import java.util.NoSuchElementException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ReferenceProperty<V> implements Property, Attachable<Property<V>>, Serializable {

    private ReferenceElement<?> element;
    private String key;

    public ReferenceProperty(final String key,final ReferenceElement<?> element) {
        this.element = element;
        this.key = key;
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
    public String key() {
        return this.key;
    }

    @Override
    public Object value() throws NoSuchElementException {
        return null;
    }

    @Override
    public boolean isPresent() {
        return false;
    }

    @Override
    public Element element() {
        return this.element;
    }

    @Override
    public void remove() {
        throw Element.Exceptions.propertyRemovalNotSupported();
    }
}
