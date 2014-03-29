package com.tinkerpop.gremlin.server;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ProfilingApplication {
    private static final int requests = 100000;
    public static void main (final String[] args) {

        try {

            final long start = System.currentTimeMillis();
            ExecutorService executor = Executors.newFixedThreadPool(8);
            final Future f1 = executor.submit(ProfilingApplication::doWork);
            final Future f2 = executor.submit(ProfilingApplication::doWork);
            final Future f3 = executor.submit(ProfilingApplication::doWork);
            final Future f4 = executor.submit(ProfilingApplication::doWork);
            final Future f5 = executor.submit(ProfilingApplication::doWork);
            final Future f6 = executor.submit(ProfilingApplication::doWork);
            final Future f7 = executor.submit(ProfilingApplication::doWork);
            final Future f8 = executor.submit(ProfilingApplication::doWork);

            f1.get();
            f2.get();
            f3.get();
            f4.get();
            f5.get();
            f6.get();
            f7.get();
            f8.get();

            final long elapsedTimeMs = (System.currentTimeMillis() - start);
            System.out.println("elapsed time (ms): " + elapsedTimeMs);
            System.out.println("request/sec: " + ((requests * 8) / (elapsedTimeMs / 1000)));

        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            System.exit(0);
        }
    }

    private static void doWork()  {
        try {
            final String url = "ws://localhost:8182/gremlin";
            final WebSocketClient client = new WebSocketClient(url);
            client.open();
            IntStream.range(0, requests).forEach(i-> {
                try {
                    final int m = i % 100;
                    System.out.println(i + ": " + client.<String>eval("1+" + m).findFirst().orElse("invalid"));
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            });
            client.close();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

    }
}
