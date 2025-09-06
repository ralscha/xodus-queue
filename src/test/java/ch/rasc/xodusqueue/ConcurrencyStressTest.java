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

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
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

		CountDownLatch startLatch = new CountDownLatch(producers + consumers);
		CountDownLatch doneLatch = new CountDownLatch(consumers);

		try (XodusBlockingQueue<String> queue = new XodusBlockingQueue<>("./stress", String.class, Long.MAX_VALUE)) {

			// track consumed items
			final Set<String> consumed = Collections.newSetFromMap(new ConcurrentHashMap<>());

			// start consumers
			for (int c = 0; c < consumers; c++) {
				new Thread(() -> {
					try {
						startLatch.countDown();
						startLatch.await();
						while (true) {
							String v = queue.poll(200, TimeUnit.MILLISECONDS);
							if (v == null) {
								// assume producers done and queue drained
								break;
							}
							consumed.add(v);
						}
					}
					catch (InterruptedException e) {
						// ignore
					}
					finally {
						doneLatch.countDown();
					}
				}).start();
			}

			// start producers
			for (int p = 0; p < producers; p++) {
				final int pid = p;
				new Thread(() -> {
					try {
						startLatch.countDown();
						startLatch.await();
						for (int i = 0; i < perProducer; i++) {
							queue.put(pid + "-" + i);
						}
					}
					catch (InterruptedException e) {
						// ignore
					}
				}).start();
			}

			// wait consumers to finish
			doneLatch.await(30, TimeUnit.SECONDS);

			// expect all items consumed
			Assertions.assertEquals(producers * perProducer, consumed.size());
		}
	}

	@Test
	public void testMultipleConcurrentPollersNoDupOrLoss() throws Exception {
		final int items = 1000;
		final int pollers = 10;

		try (XodusQueue<Integer> queue = new XodusQueue<>("./stress", Integer.class)) {
			for (int i = 0; i < items; i++) {
				queue.add(i);
			}

			final Set<Integer> seen = Collections.newSetFromMap(new ConcurrentHashMap<>());
			CountDownLatch latch = new CountDownLatch(pollers);

			for (int p = 0; p < pollers; p++) {
				new Thread(() -> {
					try {
						Integer v;
						while ((v = queue.poll()) != null) {
							seen.add(v);
						}
					}
					finally {
						latch.countDown();
					}
				}).start();
			}

			latch.await(10, TimeUnit.SECONDS);
			Assertions.assertEquals(items, seen.size());
		}
	}

}
