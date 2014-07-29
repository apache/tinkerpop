package com.tinkerpop.gremlin.process.computer.ranking.pagerank.mapreduce;

import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.ranking.pagerank.PageRankVertexProgram;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Pair;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PageRankMapReduce implements MapReduce<Object, Double, Object, Double, Iterator<Pair<Object, Double>>> {

    public boolean doStage(final Stage stage) {
        return stage.equals(Stage.MAP);
    }

    @Override
    public void map(final Vertex vertex, final MapEmitter<Object, Double> emitter) {
        final Property pageRank = vertex.property(PageRankVertexProgram.PAGE_RANK);
        if (pageRank.isPresent()) {
            emitter.emit(vertex.id(), (Double) pageRank.value());
        }
    }

    @Override
    public Iterator<Pair<Object, Double>> generateSideEffect(final Iterator<Pair<Object, Double>> keyValues) {
        return keyValues;
    }

    @Override
    public String getSideEffectKey() {
        return "pageRank";
    }
}