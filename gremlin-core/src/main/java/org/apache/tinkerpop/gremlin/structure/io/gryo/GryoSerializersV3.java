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
package org.apache.tinkerpop.gremlin.structure.io.gryo;

import org.apache.tinkerpop.gremlin.process.remote.traversal.DefaultRemoteTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Text;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.Metrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.InputShim;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.KryoShim;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.OutputShim;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.SerializerShim;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedPath;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;
import org.apache.tinkerpop.gremlin.util.function.Lambda;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;

/**
 * This class holds serializers for graph-based objects such as vertices, edges, properties, and paths. These objects
 * are "detached" using {@link DetachedFactory} before serialization. These serializers present a generalized way to
 * serialize the implementations of core interfaces. These serializers are versioned and not compatible with Gryo 1.0.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GryoSerializersV3 {

    /**
     * Serializes any {@link Edge} implementation encountered to a {@link DetachedEdge}.
     */
    public final static class EdgeSerializer implements SerializerShim<Edge> {
        @Override
        public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final Edge edge) {
            kryo.writeClassAndObject(output, edge.id());
            output.writeString(edge.label());
            kryo.writeClassAndObject(output, edge.inVertex().id());

            // temporary try/catch perhaps? need this to get SparkSingleIterationStrategyTest to work. Trying to grab
            // the label of the adjacent vertex ends in error if there's a StarEdge in the ComputerGraph$ComputerEdge.
            // apparently this is how we handle things in DetachedElement. i'll write here in the comments what was
            // written there:
            //
            // ghetto
            try {
                output.writeString(edge.inVertex().label());
            } catch (Exception ex) {
                output.writeString(Vertex.DEFAULT_LABEL);
            }

            kryo.writeClassAndObject(output, edge.outVertex().id());

            // same nonsense as above for a default label
            try {
                output.writeString(edge.outVertex().label());
            } catch (Exception ex) {
                output.writeString(Vertex.DEFAULT_LABEL);
            }

            writeElementProperties(kryo, output, edge);
        }

        @Override
        public <I extends InputShim> Edge read(final KryoShim<I, ?> kryo, final I input, final Class<Edge> edgeClass) {
            final DetachedEdge.Builder builder = DetachedEdge.build();
            builder.setId(kryo.readClassAndObject(input));
            builder.setLabel(input.readString());

            final DetachedVertex.Builder inV = DetachedVertex.build();
            inV.setId(kryo.readClassAndObject(input));
            inV.setLabel(input.readString());
            builder.setInV(inV.create());

            final DetachedVertex.Builder outV = DetachedVertex.build();
            outV.setId(kryo.readClassAndObject(input));
            outV.setLabel(input.readString());
            builder.setOutV(outV.create());

            while(input.readBoolean()) {
                builder.addProperty(new DetachedProperty<>(input.readString(), kryo.readClassAndObject(input)));
            }

            return builder.create();
        }
    }

    /**
     * Serializes any {@link Vertex} implementation encountered to an {@link DetachedVertex}.
     */
    public final static class VertexSerializer implements SerializerShim<Vertex> {
        @Override
        public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final Vertex vertex) {
            kryo.writeClassAndObject(output, vertex.id());
            output.writeString(vertex.label());

            final Iterator<? extends VertexProperty> properties = vertex.properties();
            output.writeBoolean(properties.hasNext());
            while (properties.hasNext()) {
                final VertexProperty vp = properties.next();
                kryo.writeClassAndObject(output, vp.id());
                output.writeString(vp.label());
                kryo.writeClassAndObject(output, vp.value());

                if (vp instanceof DetachedVertexProperty || (vertex.graph().features().vertex().supportsMetaProperties())) {
                    writeElementProperties(kryo, output, vp);
                } else {
                    output.writeBoolean(false);
                }

                output.writeBoolean(properties.hasNext());
            }
        }

        @Override
        public <I extends InputShim> Vertex read(final KryoShim<I, ?> kryo, final I input, final Class<Vertex> vertexClass) {
            final DetachedVertex.Builder builder = DetachedVertex.build();
            builder.setId(kryo.readClassAndObject(input));
            builder.setLabel(input.readString());

            while(input.readBoolean()) {
                final DetachedVertexProperty.Builder vpBuilder = DetachedVertexProperty.build();
                vpBuilder.setId(kryo.readClassAndObject(input));
                vpBuilder.setLabel(input.readString());
                vpBuilder.setValue(kryo.readClassAndObject(input));

                while(input.readBoolean()) {
                    vpBuilder.addProperty(new DetachedProperty<>(input.readString(), kryo.readClassAndObject(input)));
                }

                builder.addProperty(vpBuilder.create());
            }

            return builder.create();
        }
    }

    /**
     * Serializes any {@link Property} implementation encountered to an {@link DetachedProperty}.
     */
    public final static class PropertySerializer implements SerializerShim<Property> {
        @Override
        public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final Property property) {
            output.writeString(property.key());
            kryo.writeClassAndObject(output, property.value());
            kryo.writeClassAndObject(output, property.element().id());
            output.writeString(property.element().label());
        }

        @Override
        public <I extends InputShim> Property read(final KryoShim<I, ?> kryo, final I input, final Class<Property> propertyClass) {
            return new DetachedProperty<>(input.readString(), kryo.readClassAndObject(input),
                    DetachedVertex.build().setId(kryo.readClassAndObject(input)).setLabel(input.readString()).create());
        }
    }

    /**
     * Serializes any {@link VertexProperty} implementation encountered to an {@link DetachedVertexProperty}.
     */
    public final static class VertexPropertySerializer implements SerializerShim<VertexProperty> {
        @Override
        public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final VertexProperty vertexProperty) {
            kryo.writeClassAndObject(output, vertexProperty.id());
            output.writeString(vertexProperty.label());
            kryo.writeClassAndObject(output, vertexProperty.value());
            kryo.writeClassAndObject(output, vertexProperty.element().id());
            output.writeString(vertexProperty.element().label());

            if (vertexProperty instanceof DetachedVertexProperty || (vertexProperty.graph().features().vertex().supportsMetaProperties())) {
                writeElementProperties(kryo, output, vertexProperty);
            } else {
                output.writeBoolean(false);
            }
        }

        @Override
        public <I extends InputShim> VertexProperty read(final KryoShim<I, ?> kryo, final I input, final Class<VertexProperty> vertexPropertyClass) {
           final DetachedVertexProperty.Builder vpBuilder = DetachedVertexProperty.build();
            vpBuilder.setId(kryo.readClassAndObject(input));
            vpBuilder.setLabel(input.readString());
            vpBuilder.setValue(kryo.readClassAndObject(input));

            final DetachedVertex.Builder host = DetachedVertex.build();
            host.setId(kryo.readClassAndObject(input));
            host.setLabel(input.readString());
            vpBuilder.setV(host.create());

            while(input.readBoolean()) {
                vpBuilder.addProperty(new DetachedProperty<>(input.readString(), kryo.readClassAndObject(input)));
            }

            return vpBuilder.create();
        }
    }

    /**
     * Serializes any {@link Path} implementation encountered to an {@link DetachedPath}.
     */
    public final static class PathSerializer implements SerializerShim<Path> {
        @Override
        public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final Path path) {
            kryo.writeClassAndObject(output, DetachedFactory.detach(path, false));
        }

        @Override
        public <I extends InputShim> Path read(final KryoShim<I, ?> kryo, final I input, final Class<Path> pathClass) {
            return (Path) kryo.readClassAndObject(input);
        }
    }

    public final static class PSerializer implements SerializerShim<P> {
        @Override
        public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final P p) {
            output.writeString(p instanceof ConnectiveP ?
                    (p instanceof AndP ? "and" : "or") :
                    p.getBiPredicate().toString());
            if (p instanceof ConnectiveP || p.getValue() instanceof Collection) {
                output.writeByte((byte) 0);
                final Collection<?> coll = p instanceof ConnectiveP ?
                        ((ConnectiveP<?>) p).getPredicates() : (Collection) p.getValue();
                output.writeInt(coll.size());
                coll.forEach(v -> kryo.writeClassAndObject(output, v));
            } else {
                output.writeByte((byte) 1);
                kryo.writeClassAndObject(output, p.getValue());
            }
        }

        @Override
        public <I extends InputShim> P read(final KryoShim<I, ?> kryo, final I input, final Class<P> clazz) {
            final String predicate = input.readString();
            final boolean isCollection = input.readByte() == (byte) 0;
            final Object value;
            if (isCollection) {
                value = new ArrayList<>();
                final int size = input.readInt();
                for (int ix = 0; ix < size; ix++) {
                    ((List) value).add(kryo.readClassAndObject(input));
                }
            } else {
                value = kryo.readClassAndObject(input);
            }

            try {
                if (predicate.equals("and") || predicate.equals("or"))
                    return predicate.equals("and") ? new AndP((List<P>) value) : new OrP((List<P>) value);
                else if (value instanceof Collection) {
                    if (predicate.equals("between"))
                        return P.between(((List) value).get(0), ((List) value).get(1));
                    else if (predicate.equals("inside"))
                        return P.inside(((List) value).get(0), ((List) value).get(1));
                    else if (predicate.equals("outside"))
                        return P.outside(((List) value).get(0), ((List) value).get(1));
                    else if (predicate.equals("within"))
                        return P.within((Collection) value);
                    else if (predicate.equals("without"))
                        return P.without((Collection) value);
                    else
                        return (P) P.class.getMethod(predicate, Collection.class).invoke(null, (Collection) value);
                } else
                    return (P) P.class.getMethod(predicate, Object.class).invoke(null, value);
            } catch (final Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }
    }

    public final static class TextPSerializer implements SerializerShim<TextP> {
        @Override
        public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final TextP p) {
            final BiPredicate<?,?> tp = p.getBiPredicate();
            if (tp instanceof Text) {
                output.writeString(((Text) tp).name());
            } else if (tp instanceof Text.RegexPredicate) {
                output.writeString(((Text.RegexPredicate) tp).isNegate() ? "notRegex" : "regex");
            } else {
                output.writeString(tp.toString());
            }
            kryo.writeObject(output, p.getValue());
        }

        @Override
        public <I extends InputShim> TextP read(final KryoShim<I, ?> kryo, final I input, final Class<TextP> clazz) {
            final String predicate = input.readString();
            final String value = kryo.readObject(input, String.class);

            try {
                return (TextP) TextP.class.getMethod(predicate, String.class).invoke(null, value);
            } catch (final Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }
    }

    public final static class LambdaSerializer implements SerializerShim<Lambda> {
        @Override
        public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final Lambda lambda) {
            output.writeString(lambda.getLambdaScript());
            output.writeString(lambda.getLambdaLanguage());
            output.writeInt(lambda.getLambdaArguments());
        }

        @Override
        public <I extends InputShim> Lambda read(final KryoShim<I, ?> kryo, final I input, final Class<Lambda> clazz) {
            final String script = input.readString();
            final String language = input.readString();
            final int arguments = input.readInt();
            //
            if (-1 == arguments || arguments > 2)
                return new Lambda.UnknownArgLambda(script, language, arguments);
            else if (0 == arguments)
                return new Lambda.ZeroArgLambda<>(script, language);
            else if (1 == arguments)
                return new Lambda.OneArgLambda<>(script, language);
            else
                return new Lambda.TwoArgLambda<>(script, language);
        }
    }

    public final static class DefaultRemoteTraverserSerializer implements SerializerShim<DefaultRemoteTraverser> {
        @Override
        public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final DefaultRemoteTraverser remoteTraverser) {
            kryo.writeClassAndObject(output, remoteTraverser.get());
            output.writeLong(remoteTraverser.bulk());
        }

        @Override
        public <I extends InputShim> DefaultRemoteTraverser read(final KryoShim<I, ?> kryo, final I input, final Class<DefaultRemoteTraverser> remoteTraverserClass) {
            final Object o = kryo.readClassAndObject(input);
            return new DefaultRemoteTraverser<>(o, input.readLong());
        }
    }

    public final static class TraversalMetricsSerializer implements SerializerShim<TraversalMetrics> {
        @Override
        public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final TraversalMetrics object) {
            output.writeDouble(object.getDuration(TimeUnit.NANOSECONDS) / 1000000d);
            final Collection<? extends Metrics> metrics = object.getMetrics();
            output.writeInt(metrics.size());
            metrics.forEach(m -> kryo.writeObject(output, m));
        }

        @Override
        public <I extends InputShim> TraversalMetrics read(final KryoShim<I, ?> kryo, final I input, final Class<TraversalMetrics> clazz) {
            final double duration = input.readDouble();
            final int size = input.readInt();

            final List<MutableMetrics> orderedMetrics = new ArrayList<>();
            for (int ix = 0; ix < size; ix++) {
                orderedMetrics.add(kryo.readObject(input, MutableMetrics.class));
            }

            return new DefaultTraversalMetrics(Math.round(duration * 1000000), orderedMetrics);
        }
    }

    public final static class MetricsSerializer implements SerializerShim<Metrics> {
        @Override
        public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final Metrics object) {
            output.writeString(object.getId());
            output.writeString(object.getName());
            output.writeDouble(object.getDuration(TimeUnit.NANOSECONDS) / 1000000d);
            kryo.writeObject(output, object.getCounts());

            // annotations is a synchronized LinkedHashMap - get rid of the "synch" for serialization as gryo
            // doesn't know how to deserialize that well and LinkedHashMap should work with 3.3.x and previous
            final Map<String, Object> annotations = new LinkedHashMap<>();
            object.getAnnotations().forEach(annotations::put);
            kryo.writeObject(output, annotations);

            // kryo might have a problem with LinkedHashMap value collections. can't recreate it independently but
            // it gets fixed with standard collections for some reason.
            final List<Metrics> nested = new ArrayList<>(object.getNested());
            kryo.writeObject(output, nested);
        }

        @Override
        public <I extends InputShim> Metrics read(final KryoShim<I, ?> kryo, final I input, final Class<Metrics> clazz) {
            final MutableMetrics m = new MutableMetrics(input.readString(), input.readString());

            m.setDuration(Math.round(input.readDouble() * 1000000), TimeUnit.NANOSECONDS);

            final Map<String,Long> counts = (Map<String,Long>) kryo.readObject(input, HashMap.class);
            for (Map.Entry<String, Long> count : counts.entrySet()) {
                m.setCount(count.getKey(), count.getValue());
            }

            final Map<String,Object> annotations = (Map<String,Object>) kryo.readObject(input, HashMap.class);
            for (Map.Entry<String, Object> count : annotations.entrySet()) {
                m.setAnnotation(count.getKey(), count.getValue());
            }

            final List<MutableMetrics> nesteds = (List<MutableMetrics>) kryo.readObject(input, ArrayList.class);
            for (MutableMetrics nested : nesteds) {
                m.addNested(nested);
            }
            return m;
        }
    }

    private static void writeElementProperties(final KryoShim kryo, final OutputShim output, final Element element) {
        final Iterator<? extends Property> properties = element.properties();
        output.writeBoolean(properties.hasNext());
        while (properties.hasNext()) {
            final Property p = properties.next();
            output.writeString(p.key());
            kryo.writeClassAndObject(output, p.value());
            output.writeBoolean(properties.hasNext());
        }
    }
}
