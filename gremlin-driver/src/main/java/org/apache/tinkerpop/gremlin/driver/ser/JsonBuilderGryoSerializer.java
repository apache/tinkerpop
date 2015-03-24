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
package org.apache.tinkerpop.gremlin.driver.ser;

import groovy.json.JsonBuilder;
import groovy.json.JsonSlurper;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoInput;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoKryo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoOutput;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoSerializer;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class JsonBuilderGryoSerializer extends GryoSerializer<JsonBuilder> {

    final JsonSlurper slurper = new JsonSlurper();

    @Override
    public void write(final GryoKryo kryo, final GryoOutput output, final JsonBuilder jsonBuilder) {
        output.writeString(jsonBuilder.toString());
    }

    @Override
    public JsonBuilder read(final GryoKryo kryo, final GryoInput input, final Class<JsonBuilder> jsonBuilderClass) {
        final String jsonString = input.readString();
        return new JsonBuilder(slurper.parseText(jsonString));
    }
}
