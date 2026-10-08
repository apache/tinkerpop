/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.TreeMap;

/**
 * Picks the source and target vertices of the {@code alg-*} queries in {@code csr-queries.txt} from a
 * {@link CsrBenchmarkDataGenerator} power-law Gryo file, and prints the values the queries should return (top degrees,
 * component count and largest component, per-distance reach counts, unweighted and weighted distances, closed
 * three-walks) so that the results of the comparison can be checked. It is a benchmark helper, not a test.
 * <p/>
 * Usage: {@code CsrAlgorithmSources <gryo file> [pathHops=3] [weightedHops=3] [bfsHops=3]}. The vertex ids are the
 * longs {@code 0..2^scale-1}. Edges are directed; degrees count parallel edges and self-loops. Selection rules, all
 * ties broken by the lowest id:
 * <ul>
 * <li>{@code hub}: the vertex with the largest total degree (in plus out), printed with the top ten degrees and its
 * {@code both()} distance histogram for reference.</li>
 * <li>{@code bsrc}: the source of the {@code both()} distance queries: among the first 200 vertices of degree 1 by id,
 * those whose {@code both()} reach within {@code bfsHops} is at least a quarter of the graph, the one for which
 * {@code shortestPath()} sends the fewest messages (it sends every shortest path it extends to every neighbor, so from
 * a hub it sends hundreds of millions within two hops).</li>
 * <li>{@code sink}: the vertex with the largest in-degree, the target of both path queries. The generator permutes
 * sources and destinations independently, so the largest hubs have only out-edges or mostly in-edges, and a path
 * that follows {@code out()} can only end at an in-hub.</li>
 * <li>{@code src}: among the vertices whose directed distance to the sink (following {@code out()}) is exactly
 * {@code pathHops}, the one with the fewest out-walks of length 1 to {@code pathHops}, so that the hop-bounded recipe
 * {@code repeat(out().simplePath()).until(or(hasId(sink), loops().is(pathHops)))} enumerates as few paths as possible.
 * If no vertex is at that distance, the largest smaller distance is used.</li>
 * <li>{@code wsrc}: among the vertices at directed distance 2 or more from the sink whose weighted distance to the sink
 * (edge property {@code weight}, summed as doubles) is reached within {@code weightedHops} hops, the one with the
 * fewest out-walks of length 1 to {@code weightedHops}. The bounded weighted recipe and {@code shortestPath()} then
 * agree on the distance.</li>
 * </ul>
 * Reading is done through a TinkerGraph, so run it on a machine that can hold the graph (scale 18 needs a few GB).
 */
public final class CsrAlgorithmSources {

    private CsrAlgorithmSources() {
    }

    public static void main(final String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: CsrAlgorithmSources <gryo file> [pathHops=3] [weightedHops=3] [bfsHops=3]");
            System.exit(1);
        }
        final int pathHops = args.length > 1 ? Integer.parseInt(args[1]) : 3;
        final int weightedHops = args.length > 2 ? Integer.parseInt(args[2]) : 3;
        final int bfsHops = args.length > 3 ? Integer.parseInt(args[3]) : 3;

        final Configuration conf = new BaseConfiguration();
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_DEFAULT_VERTEX_PROPERTY_CARDINALITY, "list");
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_ALLOW_NULL_PROPERTY_VALUES, true);
        final TinkerGraph graph = TinkerGraph.open(conf);
        try (GraphTraversalSource g = graph.traversal()) {
            g.io(args[0]).read().iterate();
        }

        final int n = (int) graph.traversal().V().count().next().longValue();
        final int m = (int) graph.traversal().E().count().next().longValue();
        final int[] from = new int[m];
        final int[] to = new int[m];
        final double[] weight = new double[m];
        final Iterator<Edge> edges = graph.edges();
        int e = 0;
        while (edges.hasNext()) {
            final Edge edge = edges.next();
            from[e] = ((Number) edge.outVertex().id()).intValue();
            to[e] = ((Number) edge.inVertex().id()).intValue();
            weight[e] = ((Number) edge.value("weight")).doubleValue();
            e++;
        }
        graph.close();

        // out and in adjacency (with parallel edges and self-loops)
        final int[] outStart = offsets(from, n);
        final int[] inStart = offsets(to, n);
        final int[] outTo = new int[m];
        final int[] inFrom = new int[m];
        final double[] inWeight = new double[m];
        final int[] outFill = Arrays.copyOf(outStart, n);
        final int[] inFill = Arrays.copyOf(inStart, n);
        final int[] degree = new int[n];
        for (int i = 0; i < m; i++) {
            outTo[outFill[from[i]]++] = to[i];
            final int slot = inFill[to[i]]++;
            inFrom[slot] = from[i];
            inWeight[slot] = weight[i];
            degree[from[i]]++;
            degree[to[i]]++;
        }

        System.out.println("vertices=" + n + " edges=" + m);
        final Integer[] order = new Integer[n];
        for (int v = 0; v < n; v++) order[v] = v;
        Arrays.sort(order, (a, b) -> degree[a] != degree[b] ? Integer.compare(degree[b], degree[a]) : Integer.compare(a, b));
        final StringBuilder top = new StringBuilder();
        for (int i = 0; i < Math.min(10, n); i++) top.append(i == 0 ? "" : ",").append(order[i]).append(':').append(degree[order[i]]);
        System.out.println("topDegree=" + top);
        final int hub = order[0];
        System.out.println("hub=" + hub + " degree=" + degree[hub] + " outDegree=" + (outStart[hub + 1] - outStart[hub])
                + " inDegree=" + (inStart[hub + 1] - inStart[hub]));
        // the generator permutes sources and destinations independently, so the largest hubs have either only out-edges
        // or mostly in-edges; the path queries end at the largest in-hub
        int sink = 0;
        for (int v = 1; v < n; v++) if (inStart[v + 1] - inStart[v] > inStart[sink + 1] - inStart[sink]) sink = v;
        System.out.println("sink=" + sink + " degree=" + degree[sink] + " inDegree=" + (inStart[sink + 1] - inStart[sink]));

        // directed distance to the sink: BFS over in-edges from the sink
        final int[] toSink = new int[n];
        Arrays.fill(toSink, -1);
        final ArrayDeque<Integer> queue = new ArrayDeque<>();
        toSink[sink] = 0;
        queue.add(sink);
        while (!queue.isEmpty()) {
            final int v = queue.poll();
            for (int k = inStart[v]; k < inStart[v + 1]; k++) visit(toSink, queue, v, inFrom[k]);
        }
        final TreeMap<Integer, Integer> toSinkHistogram = new TreeMap<>();
        for (int v = 0; v < n; v++) if (toSink[v] >= 0) toSinkHistogram.merge(toSink[v], 1, Integer::sum);
        System.out.println("directedDistanceToSinkHistogram=" + toSinkHistogram);

        // out-walk counts, walks[l][v] = number of out-walks of length l from v
        final int maxHops = Math.max(pathHops, weightedHops);
        final double[][] walks = new double[maxHops + 1][n];
        Arrays.fill(walks[0], 1d);
        for (int l = 1; l <= maxHops; l++) {
            for (int v = 0; v < n; v++) {
                double sum = 0;
                for (int k = outStart[v]; k < outStart[v + 1]; k++) sum += walks[l - 1][outTo[k]];
                walks[l][v] = sum;
            }
        }

        int available = pathHops;
        while (available > 1 && !toSinkHistogram.containsKey(available)) available--;
        final int distance = available;
        final int src = cheapest(n, walks, distance, v -> toSink[v] == distance);
        System.out.println("src=" + src + " degree=" + degree[src] + " outDegree=" + (outStart[src + 1] - outStart[src])
                + " distanceToSink=" + toSink[src] + " hopBound=" + distance + " recipeWalks=" + (long) cost(walks, distance, src));

        // weighted distance to the sink: Dijkstra over in-edges, and the best within weightedHops hops (Bellman-Ford)
        final double[] best = new double[n];
        Arrays.fill(best, Double.POSITIVE_INFINITY);
        best[sink] = 0;
        final PriorityQueue<double[]> pq = new PriorityQueue<>((a, b) -> Double.compare(a[0], b[0]));
        pq.add(new double[]{0, sink});
        while (!pq.isEmpty()) {
            final double[] head = pq.poll();
            final int v = (int) head[1];
            if (head[0] > best[v]) continue;
            for (int k = inStart[v]; k < inStart[v + 1]; k++) {
                final double d = head[0] + inWeight[k];
                if (d < best[inFrom[k]]) {
                    best[inFrom[k]] = d;
                    pq.add(new double[]{d, inFrom[k]});
                }
            }
        }
        double[] bounded = new double[n];
        Arrays.fill(bounded, Double.POSITIVE_INFINITY);
        bounded[sink] = 0;
        for (int hop = 0; hop < weightedHops; hop++) {
            final double[] next = bounded.clone();
            for (int v = 0; v < n; v++) {
                if (bounded[v] == Double.POSITIVE_INFINITY) continue;
                for (int k = inStart[v]; k < inStart[v + 1]; k++) next[inFrom[k]] = Math.min(next[inFrom[k]], bounded[v] + inWeight[k]);
            }
            bounded = next;
        }
        final double[] boundedFinal = bounded;
        final int wsrc = cheapest(n, walks, weightedHops,
                v -> toSink[v] >= 2 && best[v] > 0 && Math.abs(best[v] - boundedFinal[v]) <= 1e-9);
        System.out.println("wsrc=" + wsrc + (wsrc < 0 ? "" : " degree=" + degree[wsrc] + " outDegree="
                + (outStart[wsrc + 1] - outStart[wsrc]) + " weightedDistance=" + best[wsrc] + " rounded1e4="
                + Math.floor(best[wsrc] * 10000 + 0.5) + " unweightedDistance=" + toSink[wsrc] + " hopBound=" + weightedHops
                + " recipeWalks=" + (long) cost(walks, weightedHops, wsrc)));

        // both() distances: from the hub for reference, and from bsrc, the source of the distance queries.
        // shortestPath() sends every shortest path it extends to every neighbor, also from the last level, so from a hub
        // it sends hundreds of millions of paths within two hops; bsrc is the degree-1 vertex (among the first
        // BFS_CANDIDATES by id) that reaches at least a quarter of the graph within bfsHops with the fewest such messages
        final int[][] neighbors = undirected(n, outStart, outTo, inStart, inFrom);
        System.out.println("fromHub " + bfs(neighbors, hub, bfsHops));
        int bsrc = -1;
        Bfs chosen = null;
        int candidates = 0;
        for (int v = 0; v < n && candidates < BFS_CANDIDATES; v++) {
            if (degree[v] != 1) continue;
            candidates++;
            final Bfs stats = bfs(neighbors, v, bfsHops);
            if (stats.reach * 4L >= n && (chosen == null || stats.messages < chosen.messages)) {
                bsrc = v;
                chosen = stats;
            }
        }
        System.out.println("bsrc=" + bsrc + (chosen == null ? "" : " " + chosen));

        // weakly connected components
        final int[] parent = new int[n];
        for (int v = 0; v < n; v++) parent[v] = v;
        for (int i = 0; i < m; i++) {
            final int a = find(parent, from[i]);
            final int b = find(parent, to[i]);
            if (a != b) parent[Math.max(a, b)] = Math.min(a, b);
        }
        final int[] size = new int[n];
        int components = 0;
        int largest = 0;
        for (int v = 0; v < n; v++) if (size[find(parent, v)]++ == 0) components++;
        for (int v = 0; v < n; v++) largest = Math.max(largest, size[v]);
        System.out.println("components=" + components + " largestComponent=" + largest);

        // closed directed walks of length three, out().out().out().where(eq('a')) (parallel edges and self-loops count)
        double walks2 = 0;
        for (int v = 0; v < n; v++) walks2 += walks[2][v];
        if (walks2 <= 2e9) {
            long closed = 0;
            for (int v = 0; v < n; v++) {
                for (int k = outStart[v]; k < outStart[v + 1]; k++) {
                    final int u = outTo[k];
                    for (int j = outStart[u]; j < outStart[u + 1]; j++) {
                        final int w = outTo[j];
                        for (int i = outStart[w]; i < outStart[w + 1]; i++) if (outTo[i] == v) closed++;
                    }
                }
            }
            System.out.println("closedThreeWalks=" + closed);
        } else {
            System.out.println("closedThreeWalks=skipped (" + (long) walks2 + " two-walks)");
        }
    }

    private static final int BFS_CANDIDATES = 200;

    private static final class Bfs {
        final TreeMap<Integer, Integer> histogram = new TreeMap<>();
        int reach;
        double paths;
        double messages;
        int hops;

        @Override
        public String toString() {
            return "bothDistanceHistogram(1.." + hops + ")=" + histogram + " reach=" + reach + " shortestVertexPaths="
                    + (long) paths + " shortestPathMessages=" + (long) messages;
        }
    }

    private static Bfs bfs(final int[][] neighbors, final int source, final int hops) {
        final int n = neighbors.length;
        final int[] dist = new int[n];
        final double[] sigma = new double[n];
        Arrays.fill(dist, -1);
        dist[source] = 0;
        sigma[source] = 1;
        final ArrayDeque<Integer> queue = new ArrayDeque<>();
        queue.add(source);
        final Bfs result = new Bfs();
        result.hops = hops;
        while (!queue.isEmpty()) {
            final int v = queue.poll();
            result.messages += sigma[v] * neighbors[v].length;
            if (v != source) {
                result.histogram.merge(dist[v], 1, Integer::sum);
                result.reach++;
                result.paths += sigma[v];
            }
            if (dist[v] == hops) continue;
            for (final int u : neighbors[v]) {
                if (dist[u] < 0) {
                    dist[u] = dist[v] + 1;
                    queue.add(u);
                }
                if (dist[u] == dist[v] + 1) sigma[u] += sigma[v];
            }
        }
        return result;
    }

    private interface VertexFilter {
        boolean test(int v);
    }

    private static double cost(final double[][] walks, final int hops, final int v) {
        double sum = 0;
        for (int l = 1; l <= hops; l++) sum += walks[l][v];
        return sum;
    }

    private static int cheapest(final int n, final double[][] walks, final int hops, final VertexFilter filter) {
        int chosen = -1;
        double chosenCost = Double.POSITIVE_INFINITY;
        for (int v = 0; v < n; v++) {
            if (!filter.test(v)) continue;
            final double c = cost(walks, hops, v);
            if (c < chosenCost) {
                chosen = v;
                chosenCost = c;
            }
        }
        return chosen;
    }

    private static int[] offsets(final int[] endpoint, final int n) {
        final int[] start = new int[n + 1];
        for (final int v : endpoint) start[v + 1]++;
        for (int i = 0; i < n; i++) start[i + 1] += start[i];
        return start;
    }

    private static int[][] undirected(final int n, final int[] outStart, final int[] outTo, final int[] inStart,
                                      final int[] inFrom) {
        final int[][] neighbors = new int[n][];
        for (int v = 0; v < n; v++) {
            final int[] all = new int[outStart[v + 1] - outStart[v] + inStart[v + 1] - inStart[v]];
            int k = 0;
            for (int i = outStart[v]; i < outStart[v + 1]; i++) all[k++] = outTo[i];
            for (int i = inStart[v]; i < inStart[v + 1]; i++) all[k++] = inFrom[i];
            Arrays.sort(all);
            int unique = 0;
            for (int i = 0; i < all.length; i++) {
                if (all[i] != v && (unique == 0 || all[unique - 1] != all[i])) all[unique++] = all[i];
            }
            neighbors[v] = Arrays.copyOf(all, unique);
        }
        return neighbors;
    }

    private static void visit(final int[] dist, final ArrayDeque<Integer> queue, final int from, final int to) {
        if (dist[to] < 0) {
            dist[to] = dist[from] + 1;
            queue.add(to);
        }
    }

    private static int find(final int[] parent, int v) {
        while (parent[v] != v) {
            parent[v] = parent[parent[v]];
            v = parent[v];
        }
        return v;
    }
}
