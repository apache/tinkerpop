package com.tinkerpop.gremlin.driver.ser;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import groovy.json.JsonBuilder;
import groovy.json.JsonSlurper;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class JsonBuilderKryoSerializer extends Serializer<JsonBuilder> {

    final JsonSlurper slurper = new JsonSlurper();

    @Override
    public void write(final Kryo kryo, final Output output, final JsonBuilder jsonBuilder) {
        output.writeString(jsonBuilder.toString());
    }

    @Override
    public JsonBuilder read(final Kryo kryo, final Input input, final Class<JsonBuilder> jsonBuilderClass) {
        final String jsonString = input.readString();
        return new JsonBuilder(slurper.parseText(jsonString));
    }
}