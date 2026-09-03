/*
 * Copyright the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ch.rasc.xodusqueue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ConcurrencyStressTest {

	@BeforeEach
	public void deleteAll() {
		TestUtil.deleteDirectory("./stress");
	}

	@AfterAll
	public static void deleteAllEnd() {
		TestUtil.deleteDirectory("./stress");
	}

	@Test
	public void testManyProducersManyConsumers() throws Exception {
		final int producers = 50;
		final int consumers = 50;
		final int perProducer = 50; // total 2500 items

		CountDownLatch startLatch = new CountDownLatch(1);
		CountDownLatch producersDone = new CountDownLatch(producers);
		ExecutorService executor = Executors.newFixedThreadPool(producers + consumers);

		XodusBlockingQueue<String> queue = new XodusBlockingQueue<>("./stress", String.class, Long.MAX_VALUE);
		try {
			final Set<String> consumed = Collections.newSetFromMap(new ConcurrentHashMap<>());
			List<Future<?>> tasks = new ArrayList<>();

			for (int c = 0; c < consumers; c++) {
				tasks.add(executor.submit(() -> {
					startLatch.await();
					while (producersDone.getCount() > 0 || !queue.isEmpty()) {
						String v = queue.poll(200, TimeUnit.MILLISECONDS);
						if (v != null) {
							Assertions.assertTrue(consumed.add(v), () -> "Duplicate element: " + v);
						}
					}
					return null;
				}));
			}

			for (int p = 0; p < producers; p++) {
				final int pid = p;
				tasks.add(executor.submit(() -> {
					try {
						startLatch.await();
						for (int i = 0; i < perProducer; i++) {
							queue.put(pid + "-" + i);
						}
					}
					finally {
						producersDone.countDown();
					}
					return null;
				}));
			}

			startLatch.countDown();
			for (Future<?> task : tasks) {
				task.get(30, TimeUnit.SECONDS);
			}

			Assertions.assertEquals(producers * perProducer, consumed.size());
			Assertions.assertTrue(queue.isEmpty());
		}
		finally {
			executor.shutdownNow();
			try {
				Assertions.assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
			}
			finally {
				queue.close();
			}
		}
	}

	@Test
	public void testMultipleConcurrentPollersNoDupOrLoss() throws Exception {
		final int items = 1000;
		final int pollers = 10;
		ExecutorService executor = Executors.newFixedThreadPool(pollers);

		XodusQueue<Integer> queue = new XodusQueue<>("./stress", Integer.class);
		try {
			for (int i = 0; i < items; i++) {
				queue.add(i);
			}

			final Set<Integer> seen = Collections.newSetFromMap(new ConcurrentHashMap<>());
			CountDownLatch startLatch = new CountDownLatch(1);
			List<Future<?>> tasks = new ArrayList<>();

			for (int p = 0; p < pollers; p++) {
				tasks.add(executor.submit(() -> {
					startLatch.await();
					Integer v;
					while ((v = queue.poll()) != null) {
						Assertions.assertTrue(seen.add(v), "Duplicate element: " + v);
					}
					return null;
				}));
			}

			startLatch.countDown();
			for (Future<?> task : tasks) {
				task.get(30, TimeUnit.SECONDS);
			}
			Assertions.assertEquals(items, seen.size());
		}
		finally {
			executor.shutdownNow();
			try {
				Assertions.assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
			}
			finally {
				queue.close();
			}
		}
	}

}
