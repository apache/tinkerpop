package com.tinkerpop.gremlin.algorithms.olap;

import com.tinkerpop.gremlin.algorithm.olap.PageRankVertexProgram;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.process.olap.ComputeResult;
import com.tinkerpop.gremlin.process.olap.MessageType;
import com.tinkerpop.gremlin.structure.query.util.VertexQueryBuilder;
import com.tinkerpop.gremlin.tinkergraph.TinkerFactory;
import junit.framework.TestCase;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PageRankVertexProgramTest extends TestCase {

    public void testPageRankProgram() throws Exception {
        Graph graph = TinkerFactory.createClassic();

        ComputeResult result = graph.compute().program(PageRankVertexProgram.create().vertexCount(6).iterations(3).build()).submit().get();
        double total = 0.0d;
        final Map<String, Double> map = new HashMap<>();
        for (Vertex vertex : graph.query().vertices()) {
            double pageRank = result.getVertexMemory().<Double>getProperty(vertex, PageRankVertexProgram.PAGE_RANK).orElse(Double.NaN);
            assertTrue(pageRank > 0.0d);
            total = total + pageRank;
            map.put(vertex.getValue("name") + " ", pageRank);
        }
        for (Map.Entry<String, Double> entry : map.entrySet()) {
            System.out.println(entry.getKey() + " " + entry.getValue());
        }

        System.out.println(total);
        System.out.println(result.getGraphMemory().getRuntime());

        /*for (int i = 1; i < 7; i++) {
            double PAGE_RANK = result.getResult(graph.getVertex(i));
            System.out.println(i + " " + (PAGE_RANK / total));
        }*/

    }

    public void testWeightedPageRankProgram() throws Exception {
        Graph graph = TinkerFactory.createClassic();

        ComputeResult result = graph.compute().program(
                PageRankVertexProgram.create()
                        .messageType(MessageType.Local.of("pageRank", new VertexQueryBuilder().direction(Direction.OUT), (Double m, Edge e) -> (m / (Float) e.getValue("weight"))))
                        .weighted(true)
                        .vertexCount(6)
                        .iterations(3)
                        .build()).submit().get();
        double total = 0.0d;
        final Map<String, Double> map = new HashMap<>();
        for (Vertex vertex : graph.query().vertices()) {
            double pageRank = result.getVertexMemory().<Double>getProperty(vertex, PageRankVertexProgram.PAGE_RANK).orElse(Double.NaN);
            assertTrue(pageRank > 0.0d);
            total = total + pageRank;
            map.put(vertex.getValue("name") + " ", pageRank);
        }
        for (Map.Entry<String, Double> entry : map.entrySet()) {
            System.out.println(entry.getKey() + " " + entry.getValue());
        }

        System.out.println(total);
        System.out.println(result.getGraphMemory().getRuntime());

        /*for (int i = 1; i < 7; i++) {
            double PAGE_RANK = result.getResult(graph.getVertex(i));
            System.out.println(i + " " + (PAGE_RANK / total));
        }*/

    }
}
