package com.tinkerpop.gremlin.structure.io.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.tinkerpop.gremlin.structure.AnnotatedList;
import com.tinkerpop.gremlin.structure.AnnotatedValue;
import com.tinkerpop.gremlin.structure.io.util.IoAnnotatedList;
import com.tinkerpop.gremlin.structure.io.util.IoAnnotatedValue;
import com.tinkerpop.gremlin.structure.util.cached.CachedAnnotatedList;
import com.tinkerpop.gremlin.structure.util.cached.CachedAnnotatedValue;

import java.util.ArrayList;
import java.util.List;

/**
 * Serializers for {@link AnnotatedList} and {@link AnnotatedValue}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class AnnotatedSerializer {

    public static class AnnotatedListSerializer extends Serializer<AnnotatedList> {

        @Override
        public void write(final Kryo kryo, final Output output, final AnnotatedList annotatedList) {
            kryo.writeClassAndObject(output, IoAnnotatedList.from(annotatedList));
        }

        @Override
        public AnnotatedList read(final Kryo kryo, final Input input, final Class<AnnotatedList> annotatedListClass) {
            final IoAnnotatedList annotatedList = (IoAnnotatedList) kryo.readClassAndObject(input);
            final List<CachedAnnotatedValue> values = new ArrayList<>();
            annotatedList.annotatedValueList.stream().map(av -> {
                final IoAnnotatedList.IoListValue lv = (IoAnnotatedList.IoListValue) av;
                return new CachedAnnotatedValue<>(lv.value, lv.annotations);
            }).forEach(cav -> values.add((CachedAnnotatedValue) cav));
            return new CachedAnnotatedList(values);
        }
    }

    public static class AnnotatedValueSerializer extends Serializer<AnnotatedValue> {

        @Override
        public void write(final Kryo kryo, final Output output, final AnnotatedValue annotatedValue) {
            kryo.writeClassAndObject(output, IoAnnotatedValue.from(annotatedValue));
        }

        @Override
        public AnnotatedValue read(final Kryo kryo, final Input input, final Class<AnnotatedValue> annotatedValueClass) {
            final IoAnnotatedValue annotatedValue = (IoAnnotatedValue) kryo.readClassAndObject(input);
            return new CachedAnnotatedValue(annotatedValue.value, annotatedValue.annotations);
        }
    }
}
