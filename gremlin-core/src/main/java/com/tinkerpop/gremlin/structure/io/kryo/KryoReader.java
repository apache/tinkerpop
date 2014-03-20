package com.tinkerpop.gremlin.structure.io.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.tinkerpop.gremlin.structure.AnnotatedList;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.io.GraphReader;
import com.tinkerpop.gremlin.structure.util.batch.BatchGraph;
import com.tinkerpop.gremlin.util.function.QuadConsumer;
import com.tinkerpop.gremlin.util.function.QuintFunction;
import com.tinkerpop.gremlin.util.function.TriFunction;
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
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.stream.IntStream;

/**
 * The {@link GraphReader} for the Gremlin Structure serialization format based on Kryo.  The format is meant to be
 * non-lossy in terms of Gremlin Structure to Gremlin Structure migrations (assuming both structure implementations
 * support the same graph features).
 * <br/>
 * This implementation is not thread-safe.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class KryoReader implements GraphReader {
    private final Kryo kryo = makeKryo();
    private final Graph graphToWriteTo;
    private final long batchSize;

    private final File tempFile;
    private final Map<Object, Object> idMap;

    final AtomicLong counter = new AtomicLong(0);

    private KryoReader(final Graph g, final Map<Object, Object> idMap, final File tempFile, final long batchSize) {
        this.graphToWriteTo = g;
        this.idMap = idMap;
        this.tempFile = tempFile;
        this.batchSize = batchSize;

        // todo: centralize kryo instance creation
        // todo: need way to register custom types.
        // todo: hardcode types
        kryo.setRegistrationRequired(true);
        kryo.register(ArrayList.class);
        kryo.register(HashMap.class);
        kryo.register(Direction.class);
        kryo.register(VertexTerminator.class);
        kryo.register(EdgeTerminator.class);
        kryo.register(KryoAnnotatedList.class);
        kryo.register(KryoAnnotatedValue.class);
    }

    @Override
    public Vertex readVertex(final InputStream inputStream, final Direction directionRequested,
                             final TriFunction<Object, String, Object[], Vertex> vertexMaker,
                             final QuintFunction<Object, Object, Object, String, Object[], Edge> edgeMaker) throws IOException {
        if (null != directionRequested && null == edgeMaker)
            throw new IllegalArgumentException("If a directionRequested is specified then an edgeAdder function should also be specified");

        final Input input = new Input(inputStream);
        readHeader(input);

        final List<Object> vertexArgs = new ArrayList<>();

        final Object vertexId = kryo.readClassAndObject(input);
        final String label = input.readString();

        final List<Pair<String, KryoAnnotatedList>> annotatedLists = readElementProperties(input, vertexArgs);
        final Vertex v = vertexMaker.apply(vertexId, label, vertexArgs.toArray());
        setAnnotatedListValues(annotatedLists, v);

        final boolean streamContainsEdgesInSomeDirection = input.readBoolean();
        if (!streamContainsEdgesInSomeDirection && Optional.ofNullable(directionRequested).isPresent())
            throw new IllegalStateException(String.format("The direction %s was requested but no attempt was made to serialize edges into this stream", directionRequested));

        // if there are edges in the stream and the direction is not present then the rest of the stream is
        // simply ignored
        if (Optional.ofNullable(directionRequested).isPresent()) {
            final Direction directionsInStream = kryo.readObject(input, Direction.class);
            if (directionsInStream != Direction.BOTH && directionsInStream != directionRequested)
                throw new IllegalStateException(String.format("Stream contains %s edges, but requesting %s", directionsInStream, directionRequested));

            final Direction firstDirection = kryo.readObject(input, Direction.class);
            if (firstDirection == Direction.OUT && (directionRequested == Direction.BOTH || directionRequested == Direction.OUT))
                readEdges(input, (eId, vId, l, properties) -> edgeMaker.apply(eId, v.getId(), vId, l, properties));
            else {
                // requested direction in, but BOTH must be serialized so skip this.  the illegalstateexception
                // prior to this IF should  have caught a problem where IN is not supported at all
                if (firstDirection == Direction.OUT && directionRequested == Direction.IN)
                    skipEdges(input);
            }

            if (directionRequested == Direction.BOTH || directionRequested == Direction.IN) {
                // if the first direction was OUT then it was either read or skipped.  in that case, the marker
                // of the stream is currently ready to read the IN direction. otherwise it's in the perfect place
                // to start reading edges
                if (firstDirection == Direction.OUT)
                    kryo.readObject(input, Direction.class);

                readEdges(input, (eId, vId, l, properties) -> edgeMaker.apply(eId, vId, v.getId(), l, properties));
            }
        }

        return v;
    }

    @Override
    public Vertex readVertex(final InputStream inputStream, final TriFunction<Object, String, Object[], Vertex> vertexMaker) throws IOException {
        return readVertex(inputStream, null, vertexMaker, null);
    }

    @Override
    public Edge readEdge(final InputStream inputStream, final QuintFunction<Object, Object, Object, String, Object[], Edge> edgeMaker) throws IOException {
        final Input input = new Input(inputStream);
        readHeader(input);
        final Object outId = kryo.readClassAndObject(input);
        final Object inId = kryo.readClassAndObject(input);
        final Object edgeId = kryo.readClassAndObject(input);
        final String label = input.readString();
        final List<Object> edgeArgs = new ArrayList<>();
        readElementProperties(input, edgeArgs);

        return edgeMaker.apply(edgeId, outId, inId, label, edgeArgs.toArray());
    }

    @Override
    public void readGraph(final InputStream inputStream) throws IOException {
        this.counter.set(0);
        final Input input = new Input(inputStream);
        readHeader(input);
        final Output output = new Output(new FileOutputStream(tempFile));

        try {
            final boolean supportedMemory = input.readBoolean();
            if (supportedMemory) {
                // todo: do we just let this fail or do we check features for supported memory types

                // if the graph that serialized the data supported memory then the memory needs to be read
                // to advance the reader forward.  if the graph being read into doesn't support the memory
                // then we just setting the data to memory.
                final Map<String,Object> memMap = (Map<String,Object>) kryo.readObject(input, HashMap.class);
                if (graphToWriteTo.getFeatures().graph().supportsMemory()) {
                    final Graph.Memory memory = graphToWriteTo.memory();
                    memMap.forEach(memory::set);
                }

                if (graphToWriteTo.getFeatures().graph().supportsTransactions())
                    graphToWriteTo.tx().commit();
            }

            final boolean hasSomeVertices = input.readBoolean();
            if (hasSomeVertices) {
                while (!input.eof()) {
                    final List<Object> vertexArgs = new ArrayList<>();
                    final Object current = kryo.readClassAndObject(input);
                    if (graphToWriteTo.getFeatures().vertex().supportsUserSuppliedIds())
                        vertexArgs.addAll(Arrays.asList(Element.ID, current));

                    vertexArgs.addAll(Arrays.asList(Element.LABEL, input.readString()));
                    final List<Pair<String, KryoAnnotatedList>> annotatedLists = readElementProperties(input, vertexArgs);

                    final Vertex v = graphToWriteTo.addVertex(vertexArgs.toArray());

                    // annotated list properties are set after the fact
                    setAnnotatedListValues(annotatedLists, v);

                    idMap.put(current, v.getId());

                    considerCommit();

                    // the gio file should have been written with a direction specified
                    final boolean hasDirectionSpecified = input.readBoolean();
                    final Direction directionInStream = kryo.readObject(input, Direction.class);
                    final Direction directionOfEdgeBatch = kryo.readObject(input, Direction.class);

                    // graph serialization requires that a direction be specified in the stream and that the
                    // direction of the edges be OUT
                    if (!hasDirectionSpecified || directionInStream != Direction.OUT || directionOfEdgeBatch != Direction.OUT)
                        throw new IllegalStateException(String.format("Stream must specify edge direction and that direction must be %s", Direction.OUT));

                    // if there are edges then read them to end and write to temp, otherwise read what should be
                    // the vertex terminator
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
            if (graphToWriteTo.getFeatures().graph().supportsTransactions())
                graphToWriteTo.tx().commit();
            edgeInput.close();
            deleteTempFileSilently();
        }
    }

    private void considerCommit() {
        if (graphToWriteTo.getFeatures().graph().supportsTransactions() &&
            counter.incrementAndGet() % batchSize == 0)
            graphToWriteTo.tx().commit();
    }

    private void readHeader(final Input input) throws IOException {
        if (!Arrays.equals(KryoWriter.GIO, input.readBytes(3)))
            throw new IOException("Invalid format - first three bytes of header do not match expected value");

        // skip the next 26 bytes in v1
        input.readBytes(26);

        // final three bytes of header are the version which should be 1.0.0
        byte[] version = input.readBytes(3);

        // direct match on version for now
        if (version[0] != 1 || version[1] != 0 || version[2] != 0)
            throw new IOException(String.format(
                    "The version [%s.%s.%s] in the stream cannot be understood by this reader",
                    version[0], version[1], version[2]));
    }

    private void readEdges(final Input input, final QuadConsumer<Object, Object, String, Object[]> edgeMaker) {
        if (input.readBoolean()) {
            Object inOrOutVId = kryo.readClassAndObject(input);
            while (!inOrOutVId.equals(EdgeTerminator.INSTANCE)) {
                final List<Object> edgeArgs = new ArrayList<>();
                final Object edgeId = kryo.readClassAndObject(input);
                final String edgeLabel = input.readString();
                readElementProperties(input, edgeArgs);

                edgeMaker.accept(edgeId, inOrOutVId, edgeLabel, edgeArgs.toArray());

                inOrOutVId = kryo.readClassAndObject(input);
            }
        }
    }

    private void skipEdges(final Input input) {
        if (input.readBoolean()) {
            Object inOrOutId = kryo.readClassAndObject(input);
            while (!inOrOutId.equals(EdgeTerminator.INSTANCE)) {
                // skip edgeid
                kryo.readClassAndObject(input);

                // skip label
                input.readString();

                // read property count so we know how many properties to skip
                final int numberOfProperties = input.readInt();
                IntStream.range(0, numberOfProperties).forEach(i -> {
                    input.readString();
                    kryo.readClassAndObject(input);
                });

                // next in/out id to skip
                inOrOutId = kryo.readClassAndObject(input);
            }
        }
    }

    private void setAnnotatedListValues(final List<Pair<String, KryoAnnotatedList>> annotatedLists, final Vertex v) {
        annotatedLists.forEach(kal -> {
            final AnnotatedList al = v.getValue(kal.getValue0());
            final List<KryoAnnotatedValue> valuesForAnnotation = kal.getValue1().getAnnotatedValueList();
            for (KryoAnnotatedValue kav : valuesForAnnotation) {
                al.addValue(kav.getValue(), kav.getAnnotationsArray());
            }
        });
    }

    /**
     * Reads through the all the edges for a vertex and writes the edges to a temp file which will be read later.
     */
    private void readToEndOfEdgesAndWriteToTemp(final Input input, final Output output) throws IOException {
        Object inId = kryo.readClassAndObject(input);
        while (!inId.equals(EdgeTerminator.INSTANCE)) {
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

        // this should be the vertex terminator
        kryo.readClassAndObject(input);

        kryo.writeClassAndObject(output, EdgeTerminator.INSTANCE);
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
            while (!inId.equals(EdgeTerminator.INSTANCE)) {
                final List<Object> edgeArgs = new ArrayList<>();
                final Vertex vOut = graphToWriteTo.v(outId);

                final Object edgeId = kryo.readClassAndObject(input);
                if (graphToWriteTo.getFeatures().edge().supportsUserSuppliedIds())
                    edgeArgs.addAll(Arrays.asList(Element.ID, edgeId));

                final String edgeLabel = input.readString();
                final Vertex inV = graphToWriteTo.v(idMap.get(inId));
                readElementProperties(input, edgeArgs);

                vOut.addEdge(edgeLabel, inV, edgeArgs.toArray());
                considerCommit();

                inId = kryo.readClassAndObject(input);
            }

            // vertex terminator
            kryo.readClassAndObject(input);
        }
    }

    /**
     * Read element properties from input stream and put them into an argument list.  Properties that have an
     * {@link AnnotatedList} as a value have their data returned to be added once it is added to the graph.
     *
     * @return a list of keys that are {@link AnnotatedList} values which must be set after the property is added
     * to the vertex
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
        private long batchSize = BatchGraph.DEFAULT_BUFFER_SIZE;

        public Builder(final Graph g) {
            this.g = g;
            this.idMap = new HashMap<>();
            this.tempFile = new File(UUID.randomUUID() + ".tmp");
        }

        public Builder batchSize(final long batchSize) {
            this.batchSize = batchSize;
            return this;
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
            return new KryoReader(g, idMap, tempFile, batchSize);
        }
    }

    private static Kryo makeKryo() {
        return new Kryo();
    }
}
