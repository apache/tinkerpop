package com.tinkerpop.gremlin.computer;

import junit.framework.TestCase;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PageRankVertexProgramTest extends TestCase {

    public void testTrue() {

    }

    /*public void testPageRankProgram() throws Exception {
        Graph graph = TinkerFactory.createClassic();

        PageRankVertexProgram program = PageRankVertexProgram.create().vertexCount(6).iterations(3).build();
        ComputeResult result = graph.compute().program(program).submit();
        double total = 0.0d;
        final Map<String, Double> map = new HashMap<String, Double>();
        for (Vertex vertex : graph.query().vertices()) {
            double pageRank = Double.parseDouble(result.getVertexMemory().getProperty(vertex, PageRankVertexProgram.PAGE_RANK).getValue().toString());
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
        }

    }*/
}