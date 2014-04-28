package com.tinkerpop.gremlin.structure.io.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.tinkerpop.gremlin.structure.AnnotatedList;
import com.tinkerpop.gremlin.structure.AnnotatedValue;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.io.util.IOAnnotatedList;
import com.tinkerpop.gremlin.structure.io.util.IOAnnotatedValue;
import com.tinkerpop.gremlin.structure.io.util.IOVertex;
import com.tinkerpop.gremlin.structure.util.cached.CachedAnnotatedList;
import com.tinkerpop.gremlin.structure.util.cached.CachedAnnotatedValue;
import com.tinkerpop.gremlin.structure.util.cached.CachedVertex;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class AnnotatedSerializer {

    public static class AnnotatedListSerializer extends Serializer<AnnotatedList> {

        @Override
        public void write(final Kryo kryo, final Output output, final AnnotatedList annotatedList) {
            kryo.writeClassAndObject(output, IOAnnotatedList.from(annotatedList));
        }

        @Override
        public AnnotatedList read(final Kryo kryo, final Input input, final Class<AnnotatedList> annotatedListClass) {
            final IOAnnotatedList annotatedList = (IOAnnotatedList) kryo.readClassAndObject(input);
            final List<CachedAnnotatedValue> values = new ArrayList<>();
            annotatedList.getAnnotatedValueList().stream().map(av -> {
                final IOAnnotatedList.IOListValue lv = (IOAnnotatedList.IOListValue) av;
                return new CachedAnnotatedValue<>(lv.getValue(), lv.getAnnotations());
            }).forEach(cav -> values.add((CachedAnnotatedValue) cav));
            return new CachedAnnotatedList(values);
        }
    }

    public static class AnnotatedValueSerializer extends Serializer<AnnotatedValue> {

        @Override
        public void write(final Kryo kryo, final Output output, final AnnotatedValue annotatedValue) {
            kryo.writeClassAndObject(output, IOAnnotatedValue.from(annotatedValue));
        }

        @Override
        public AnnotatedValue read(final Kryo kryo, final Input input, final Class<AnnotatedValue> annotatedValueClass) {
            final IOAnnotatedValue annotatedValue = (IOAnnotatedValue) kryo.readClassAndObject(input);
            return new CachedAnnotatedValue(annotatedValue.getValue(), annotatedValue.getAnnotations());
        }
    }
}
