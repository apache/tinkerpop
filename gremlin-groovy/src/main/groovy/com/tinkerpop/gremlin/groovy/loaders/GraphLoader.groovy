package com.tinkerpop.gremlin.groovy.loaders

import com.tinkerpop.gremlin.structure.Graph
import com.tinkerpop.gremlin.structure.io.graphml.GraphMLReader
import com.tinkerpop.gremlin.structure.io.graphml.GraphMLWriter
import com.tinkerpop.gremlin.structure.io.graphson.GraphSONReader
import com.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter
import com.tinkerpop.gremlin.structure.io.kryo.KryoReader
import com.tinkerpop.gremlin.structure.io.kryo.KryoWriter

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GraphLoader {

    public static void load() {

        // GraphML loading and saving
        Graph.metaClass.loadGraphML = { final def fileObject ->
            final GraphMLReader reader = GraphMLReader.build().create();
            try {
                reader.readGraph(new URL(fileObject).openStream(), (Graph) delegate);
            } catch (final MalformedURLException e) {
                reader.readGraph(new FileInputStream(fileObject), (Graph) delegate)
            }
        }

        Graph.metaClass.saveGraphML = { final def fileObject ->
            GraphMLWriter.build().create().writeGraph(new FileOutputStream(fileObject), (Graph) delegate)
        }

        // GraphSON loading and saving
        Graph.metaClass.loadGraphSON = { final def fileObject ->
            final GraphSONReader reader = GraphSONReader.build().create();
            try {
                reader.readGraph(new URL(fileObject).openStream(), (Graph) delegate);
            } catch (final MalformedURLException e) {
                reader.readGraph(new FileInputStream(fileObject), (Graph) delegate)
            }
        }

        Graph.metaClass.saveGraphSON = { final def fileObject ->
            GraphSONWriter.build().create().writeGraph(new FileOutputStream(fileObject), (Graph) delegate)
        }

        // Kryo loading and saving
        Graph.metaClass.loadKryo = { final def fileObject ->
            final KryoReader reader = KryoReader.build().create();
            try {
                reader.readGraph(new URL(fileObject).openStream(), (Graph) delegate);
            } catch (final MalformedURLException e) {
                reader.readGraph(new FileInputStream(fileObject), (Graph) delegate)
            }
        }

        Graph.metaClass.saveKryo = { final def fileObject ->
            KryoWriter.build().create().writeGraph(new FileOutputStream(fileObject), (Graph) delegate)
        }


    }
}
