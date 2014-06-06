package com.tinkerpop.gremlin.driver.benchmark;

import com.tinkerpop.gremlin.driver.Client;
import com.tinkerpop.gremlin.driver.Cluster;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ProfilingApplication {
	public static void main(final String[] args) {
		try {
			System.out.println("Initializing at: " + System.nanoTime());

			final int clients = 2;
			final int requests = 10000;
			final Cluster cluster = Cluster.create("54.198.44.194")
					.minConnectionPoolSize(512)
					.maxConnectionPoolSize(1024)
					.minSimultaneousRequestsPerConnection(64)
					.maxSimultaneousRequestsPerConnection(128)
					.maxInProcessPerConnection(8)
					.nioPoolSize(clients)
					.workerPoolSize(clients * 4).build();

			// let all the clients fully init before starting to send messages
			final CyclicBarrier barrier = new CyclicBarrier(clients);

			final List<Thread> threads = IntStream.range(0, clients).mapToObj(t -> new Thread(() -> {
				try {
					final CountDownLatch latch = new CountDownLatch(requests);

					final Client client = cluster.connect();
					client.init();

					barrier.await();
					final long start = System.nanoTime();

					System.out.println("Executing at [" + t + "]:" + start);

					IntStream.range(0, requests).forEach(i -> {
						client.submitAsync("1+1").thenAccept(r -> r.all()).thenRun(latch::countDown);
					});

					latch.await();

					final long end = System.nanoTime();
					final long total = end - start;

					System.out.println("All responses for [" + t + "] are accounted for at: " + end);

					final long totalSeconds = Math.round(total / 1000000000d);
					final long requestCount = requests;
					final long reqSec = Math.round(requestCount / totalSeconds);
					System.out.println(String.format("[" + t + "] clients: %s requests: %s time(s): %s req/sec: %s", clients, requestCount, totalSeconds, reqSec));
				} catch (Exception ex) {
					ex.printStackTrace();
					throw new RuntimeException(ex);
				}
			})).collect(Collectors.toList());

			threads.forEach(t -> {
				try {
					t.start();
				} catch (Exception ex) {
					ex.printStackTrace();
					throw new RuntimeException(ex);
				}
			});

			threads.forEach(t -> {
				try {
					t.join();
				} catch (Exception ex) {
					ex.printStackTrace();
					throw new RuntimeException(ex);
				}
			});

		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			System.exit(0);
		}
	}
}
