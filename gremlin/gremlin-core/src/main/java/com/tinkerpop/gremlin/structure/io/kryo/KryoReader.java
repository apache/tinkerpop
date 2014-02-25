package com.tinkerpop.gremlin.structure.io.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.tinkerpop.gremlin.structure.AnnotatedList;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.io.GraphReader;
import org.javatuples.Pair;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class KryoReader implements GraphReader {
    private final Kryo kryo = new Kryo();
    private final Graph graph;

    private final File tempFile;
    private final Map<Object, Object> idMap;

    private KryoReader(final Graph g, final Map<Object, Object> idMap, final File tempFile) {
        this.graph = g;
        this.idMap = idMap;
        this.tempFile = tempFile;
    }
    @Override
    public void readGraph(final InputStream inputStream) throws IOException {
        final Input input = new Input(inputStream);
        final Output output = new Output(new FileOutputStream(tempFile));

        try {
            final boolean supportedAnnotations = input.readBoolean();
            if (supportedAnnotations && graph.getFeatures().graph().supportsAnnotations()) {
                final Graph.Annotations annotations = graph.annotations();
                final Map<String,Object> annotationMap = (Map<String,Object>) kryo.readObject(input, HashMap.class);
                annotationMap.forEach(annotations::set);
            }

            final boolean hasSomeVertices = input.readBoolean();
            if (hasSomeVertices) {
                while (!input.eof()) {
                    final List<Object> vertexArgs = new ArrayList<>();
                    final Object current = kryo.readClassAndObject(input);
                    if (graph.getFeatures().vertex().supportsUserSuppliedIds())
                        vertexArgs.addAll(Arrays.asList(Element.ID, current));

                    vertexArgs.addAll(Arrays.asList(Element.LABEL, input.readString()));
                    readElementProperties(input, vertexArgs);

                    final Vertex v = graph.addVertex(vertexArgs.toArray());
                    idMap.put(current, v.getId());

                    // if there are edges then read them to end and write to temp otherwise, read what should be
                    // the terminator
                    if (!input.readBoolean())
                        kryo.readClassAndObject(input);
                    else {
                        // writes the real new id of the outV to the temp.  only need to write vertices to temp that
                        // have edges.  no need to reprocess those that don't again.
                        kryo.writeClassAndObject(output, v.getId());
                        readToEndOfEdgesAndWriteToTemp(input, output);
                    }

                }
            }
        } finally {
            // done writing to temp
            output.close();
        }

        // start reading in the edges now from the temp file
        final Input edgeInput = new Input(new FileInputStream(tempFile));
        try {
            readFromTempEdges(edgeInput);
        } finally {
            edgeInput.close();
            deleteTempFileSilently();
        }
    }

    /**
     * Reads through the all the edges for a vertex and writes the edges to a temp file which will be read later.
     */
    private void readToEndOfEdgesAndWriteToTemp(final Input input, final Output output) throws IOException {
        Object inId = kryo.readClassAndObject(input);
        while (!inId.equals(VertexTerminator.INSTANCE)) {
            kryo.writeClassAndObject(output, inId);

            // edge id
            kryo.writeClassAndObject(output, kryo.readClassAndObject(input));

            // label
            output.writeString(input.readString());
            final int props = input.readInt();
            output.writeInt(props);
            IntStream.range(0, props).forEach(i-> {
                // key
                output.writeString(input.readString());

                // value
                kryo.writeClassAndObject(output, kryo.readClassAndObject(input));
            });

            // next inId or terminator
            inId = kryo.readClassAndObject(input);
        }

        kryo.writeClassAndObject(output, VertexTerminator.INSTANCE);
    }

    /**
     * Read the edges from the temp file and load them to the graph.
     */
    private void readFromTempEdges(final Input input) {
        while (!input.eof()) {
            // in this case the outId is the id assigned by the graph
            final Object outId = kryo.readClassAndObject(input);
            Object inId = kryo.readClassAndObject(input);
            while (!inId.equals(VertexTerminator.INSTANCE)) {
                final List<Object> edgeArgs = new ArrayList<>();
                final Vertex vOut = graph.v(outId);

                final Object edgeId = kryo.readClassAndObject(input);
                if (graph.getFeatures().edge().supportsUserSuppliedIds())
                    edgeArgs.addAll(Arrays.asList(Element.ID, edgeId));

                final String edgeLabel = input.readString();
                final Vertex inV = graph.v(idMap.get(inId));
                final List<Pair<String, KryoAnnotatedList>> annotatedLists = readElementProperties(input, edgeArgs);
                annotatedLists.forEach(kal -> {
                    final AnnotatedList al = inV.getValue(kal.getValue0());
                    final List<KryoAnnotatedValue> valuesForAnnotation = kal.getValue1().getAnnotatedValueList();
                    for (KryoAnnotatedValue kav : valuesForAnnotation) {
                        al.addValue(kav.getValue(), kav.getAnnotationsArray());
                    }
                });

                vOut.addEdge(edgeLabel, inV, edgeArgs.toArray());

                inId = kryo.readClassAndObject(input);
            }
        }
    }

    /**
     * Read element properties from input stream and put them into an argument list.
     */
    private List<Pair<String, KryoAnnotatedList>> readElementProperties(final Input input, final List<Object> elementArgs) {
        // todo: do we just let this fail or do we check features for supported property types
        final List<Pair<String, KryoAnnotatedList>> list = new ArrayList<>();
        final int numberOfProperties = input.readInt();
        IntStream.range(0, numberOfProperties).forEach(i -> {
            final String key = input.readString();
            elementArgs.add(key);
            final Object val = kryo.readClassAndObject(input);
            if (val instanceof KryoAnnotatedList) {
                elementArgs.add(AnnotatedList.make());
                list.add(Pair.with(key, (KryoAnnotatedList) val));
            } else
                elementArgs.add(val);
        });

        return list;
    }

    private void deleteTempFileSilently() {
        try {
            tempFile.delete();
        } catch (Exception ex) { }
    }

    public static class Builder {
        private Graph g;
        private Map<Object, Object> idMap;
        private File tempFile;

        public Builder(final Graph g) {
            this.g = g;
            this.idMap = new HashMap<>();
            this.tempFile = new File(UUID.randomUUID() + ".tmp");
        }

        /**
         * A {@link Map} implementation that will handle vertex id translation in the event that the graph does
         * not support identifier assignment. If this value is not set, it uses a standard HashMap.
         */
        public Builder setIdMap(final Map<Object, Object> idMap) {
            this.idMap = idMap;
            return this;
        }

        /**
         * The reader requires a working directory to write temp files to.  If this value is not set, it will write
         * the temp file to the local directory.
         */
        public Builder setWorkingDirectory(final String workingDirectory) {
            final File f = new File(workingDirectory);
            if (!f.exists() || !f.isDirectory())
                throw new IllegalArgumentException("The workingDirectory is not a directory or does not exist");

            tempFile = new File(workingDirectory + File.separator + UUID.randomUUID() + ".tmp");
            return this;
        }

        public KryoReader build() {
            return new KryoReader(g, idMap, tempFile);
        }
    }
}
