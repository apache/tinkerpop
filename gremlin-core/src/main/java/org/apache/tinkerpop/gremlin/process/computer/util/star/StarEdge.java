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

package org.apache.tinkerpop.gremlin.process.computer.util.star;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class StarEdge extends StarElement implements Edge {

    private Map<String, Object> properties = null;

    public StarEdge(final Object id, final String label) {
        super(id, label);
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        if (null == this.properties)
            this.properties = new HashMap<>();
        this.properties.put(key, value);
        return new StarProperty<>(key, value,this);
    }

    @Override
    public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
        return null == this.properties ?
                Collections.emptyIterator() :
                (Iterator) this.properties.entrySet()
                        .stream()
                        .filter(entry -> ElementHelper.keyExists(entry.getKey(), propertyKeys))
                        .map(entry -> new StarProperty<>(entry.getKey(), (V) entry.getValue(),this))
                        .iterator();
    }

    @Override
    public void remove() {
        //TODO: throw Edge.Exceptions.edgeRemovalNotSupported();
    }

    @Override
    public boolean equals(final Object other) {
        return ElementHelper.areEqual(this, other);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }
}
