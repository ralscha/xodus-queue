/**
 * Copyright 2018-2018 the original author or authors.
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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class XodusBlockingQueueTest {

	@BeforeEach
	public void deleteAll() {
		TestUtil.deleteDirectory("./blockingtest");
	}

	@AfterAll
	public static void deleteAllEnd() {
		TestUtil.deleteDirectory("./blockingtest");
	}

	@Test
	void testOfferTake() {
		try (XodusBlockingQueue<String> queue = new XodusBlockingQueue<>("./blockingtest",
				String.class, Long.MAX_VALUE)) {

			CountDownLatch countDown = new CountDownLatch(10);

			Thread producer = new Thread(() -> {
				for (int i = 0; i < 10; i++) {
					try {
						System.out.println("producer: " + i);
						queue.offer(String.valueOf(i));
						TimeUnit.MILLISECONDS.sleep(50);
					}
					catch (InterruptedException e) {
						Assertions.fail(e);
					}
				}
			});

			Thread consumer = new Thread(() -> {
				for (int i = 0; i < 10; i++) {
					try {
						Assertions.assertEquals(String.valueOf(i), queue.take());
						System.out.println(i);
						countDown.countDown();
					}
					catch (InterruptedException e) {
						Assertions.fail(e);
					}
				}
			});

			consumer.start();
			producer.start();
			try {
				countDown.await();
			}
			catch (InterruptedException e) {
				Assertions.fail(e);
			}
		}
	}

	@Test
	void testPollLongTimeUnit() {
		try (XodusBlockingQueue<Integer> queue = new XodusBlockingQueue<>(
				"./blockingtest", Integer.class, Long.MAX_VALUE)) {

			CountDownLatch countDown = new CountDownLatch(10);

			Thread producer = new Thread(() -> {
				for (int i = 0; i < 10; i++) {
					try {
						System.out.println("producer: " + i);
						queue.offer(i, 1, TimeUnit.SECONDS);
						TimeUnit.MILLISECONDS.sleep(50);
					}
					catch (InterruptedException e) {
						Assertions.fail(e);
					}
				}
			});

			Thread consumer = new Thread(() -> {
				try {
					Assertions.assertNull(queue.poll(50, TimeUnit.MILLISECONDS));
				}
				catch (InterruptedException e) {
					Assertions.fail(e);
				}

				for (int i = 0; i < 10; i++) {
					try {
						Assertions.assertEquals(i,
								(int) queue.poll(300, TimeUnit.MILLISECONDS));
						System.out.println(i);
						countDown.countDown();
					}
					catch (InterruptedException e) {
						Assertions.fail(e);
					}
				}
			});

			try {
				consumer.start();
				TimeUnit.MILLISECONDS.sleep(200);
				producer.start();
				countDown.await();
			}
			catch (InterruptedException e) {
				Assertions.fail(e);
			}
		}
	}

	@Test
	void testAddAllCollectionOfQextendsT() {
		try (XodusBlockingQueue<Long> queue = new XodusBlockingQueue<>("./blockingtest",
				Long.class, Long.MAX_VALUE)) {

			CountDownLatch countDown = new CountDownLatch(6);

			Thread producer = new Thread(() -> {
				queue.addAll(Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L));
			});

			Thread consumer = new Thread(() -> {
				for (int i = 1; i <= 6; i++) {
					try {
						Assertions.assertEquals(i, (long) queue.take());
						countDown.countDown();
					}
					catch (InterruptedException e) {
						Assertions.fail(e);
					}
				}
			});

			try {
				consumer.start();
				TimeUnit.MILLISECONDS.sleep(300);
				producer.start();
				countDown.await();

				Assertions.assertEquals(0, queue.size());
			}
			catch (InterruptedException e) {
				Assertions.fail(e);
			}
		}
	}

	@Test
	void testRemainingCapacity() {
		try (XodusBlockingQueue<String> queue = new XodusBlockingQueue<>("./blockingtest",
				String.class, Long.MAX_VALUE)) {
			Assertions.assertEquals(Integer.MAX_VALUE, queue.remainingCapacity());
		}
	}

	@Test
	void testDrainToCollectionOfQsuperT() {
		try (XodusBlockingQueue<Long> queue = new XodusBlockingQueue<>("./blockingtest",
				Long.class, Long.MAX_VALUE)) {
			queue.offer(11L);
			queue.offer(22L);
			queue.offer(33L);
			queue.offer(44L);

			List<Long> c = new ArrayList<>();
			queue.drainTo(c);

			Assertions.assertEquals(4, c.size());
			Assertions.assertEquals(11L, (long) c.get(0));
			Assertions.assertEquals(22L, (long) c.get(1));
			Assertions.assertEquals(33L, (long) c.get(2));
			Assertions.assertEquals(44L, (long) c.get(3));
			Assertions.assertEquals(0, queue.size());
		}
	}

	@Test
	void testDrainToCollectionOfQsuperTInt() {
		try (XodusBlockingQueue<Long> queue = new XodusBlockingQueue<>("./blockingtest",
				Long.class, Long.MAX_VALUE)) {
			queue.offer(11L);
			queue.offer(22L);
			queue.offer(33L);
			queue.offer(44L);

			List<Long> c = new ArrayList<>();
			queue.drainTo(c, 1);

			Assertions.assertEquals(1, c.size());
			Assertions.assertEquals(11L, (long) c.get(0));

			Assertions.assertEquals(3, queue.size());

			c.clear();
			queue.drainTo(c, 2);
			Assertions.assertEquals(2, c.size());
			Assertions.assertEquals(22L, (long) c.get(0));
			Assertions.assertEquals(33L, (long) c.get(1));

			c.clear();
			queue.drainTo(c, 2);
			Assertions.assertEquals(1, c.size());
			Assertions.assertEquals(44L, (long) c.get(0));
		}
	}

}
