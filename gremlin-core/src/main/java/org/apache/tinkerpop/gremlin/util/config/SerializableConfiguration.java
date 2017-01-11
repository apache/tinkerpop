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

package org.apache.tinkerpop.gremlin.util.config;

import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.configuration.Configuration;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SerializableConfiguration extends AbstractConfiguration implements Serializable {

    private final Map<String, Object> properties = new HashMap<>();

    public SerializableConfiguration() {
        super();
        super.setDelimiterParsingDisabled(true);
    }

    public SerializableConfiguration(final Configuration configuration) {
        this();
        this.copy(configuration);
    }

    @Override
    protected void addPropertyDirect(final String key, final Object value) {
        this.properties.put(key, value);
    }

    @Override
    protected void clearPropertyDirect(final String key) {
        this.properties.remove(key);
    }

    @Override
    public boolean isEmpty() {
        return this.properties.isEmpty();
    }

    @Override
    public boolean containsKey(final String key) {
        return this.properties.containsKey(key);
    }

    @Override
    public Object getProperty(final String key) {
        return this.properties.get(key);
    }

    @Override
    public Iterator<String> getKeys() {
        return this.properties.keySet().iterator();
    }

}
