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
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class XodusBlockingQueueCapacityTest {

	@BeforeEach
	public void deleteAll() {
		TestUtil.deleteDirectory("./blockingtest");
	}

	@AfterAll
	public static void deleteAllEnd() {
		TestUtil.deleteDirectory("./blockingtest");
	}

	@Test
	void testShutdown() {
		try (XodusBlockingQueue<String> queue = new XodusBlockingQueue<>("./blockingtest", String.class, 4)) {
			queue.add("one");
			queue.add("two");
			queue.add("three");
			Assertions.assertEquals(3, queue.size());
		}

		try (XodusBlockingQueue<String> queue = new XodusBlockingQueue<>("./blockingtest", String.class, 4)) {
			queue.add("four");
			Assertions.assertEquals(4, queue.size());
		}

		try (XodusBlockingQueue<String> queue = new XodusBlockingQueue<>("./blockingtest", String.class, 4)) {
			Assertions.assertEquals("one", queue.remove());
			Assertions.assertEquals(3, queue.size());
		}
	}

	@Test
	void testClear() {
		try (XodusBlockingQueue<String> queue = new XodusBlockingQueue<>("./blockingtest", String.class, 3)) {

			CountDownLatch countDown1 = new CountDownLatch(3);
			CountDownLatch countDown2 = new CountDownLatch(3);

			Thread producer = new Thread(() -> {
				for (int i = 0; i < 3; i++) {
					try {
						queue.put(String.valueOf(i));
					}
					catch (InterruptedException e) {
						Assertions.fail(e);
					}
					countDown1.countDown();
				}

				for (int i = 0; i < 3; i++) {
					try {
						queue.put(String.valueOf(i));
					}
					catch (InterruptedException e) {
						Assertions.fail(e);
					}
					countDown2.countDown();
				}
			});

			try {
				producer.start();
				countDown1.await();
				TimeUnit.SECONDS.sleep(1);
				queue.clear();

				countDown2.await();
				Assertions.assertEquals(3, queue.size());
			}
			catch (InterruptedException e) {
				Assertions.fail(e);
			}
		}

	}

	@Test
	void testAddT() {
		try (XodusBlockingQueue<Long> queue = new XodusBlockingQueue<>("./blockingtest", Long.class, 1)) {
			Assertions.assertTrue(queue.add(1L));
			Assertions.assertThrows(IllegalStateException.class, () -> queue.add(2L));
			Assertions.assertEquals(1L, (long) queue.remove());
			Assertions.assertTrue(queue.add(2L));
			Assertions.assertEquals(2L, (long) queue.remove());
		}
	}

	@Test
	void testOfferT() {
		try (XodusBlockingQueue<Long> queue = new XodusBlockingQueue<>("./blockingtest", Long.class, 1)) {
			Assertions.assertTrue(queue.offer(1L));
			Assertions.assertFalse(queue.offer(2L));
			Assertions.assertEquals(1L, (long) queue.remove());
			Assertions.assertTrue(queue.offer(2L));
			Assertions.assertEquals(2L, (long) queue.remove());
		}
	}

	@Test
	void testOfferTLongTimeUnit() {
		try (XodusBlockingQueue<Long> queue = new XodusBlockingQueue<>("./blockingtest", Long.class, 1)) {
			try {
				Assertions.assertTrue(queue.offer(1L, 2, TimeUnit.SECONDS));
				Assertions.assertFalse(queue.offer(2L, 2, TimeUnit.SECONDS));
			}
			catch (InterruptedException e) {
				Assertions.fail(e);
			}
		}
	}

	@Test
	void testAddAllCollectionOfQextendsT() {
		try (XodusBlockingQueue<Long> queue = new XodusBlockingQueue<>("./blockingtest", Long.class, 3)) {
			Assertions.assertThrows(IllegalStateException.class,
					() -> queue.addAll(Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L)));
			Assertions.assertEquals(3, queue.size());

			queue.clear();
			boolean modified = queue.addAll(Arrays.asList(1L, 2L, 3L));
			Assertions.assertEquals(true, modified);
			Assertions.assertEquals(3, queue.size());
		}
	}

	@Test
	void testRemove() {
		try (XodusBlockingQueue<Long> queue = new XodusBlockingQueue<>("./blockingtest", Long.class, 1)) {
			Assertions.assertThrows(NoSuchElementException.class, () -> queue.remove());

			Assertions.assertTrue(queue.offer(1L));
			Assertions.assertEquals(1L, (long) queue.remove());
		}
	}

	@Test
	void testPoll() {
		try (XodusBlockingQueue<Long> queue = new XodusBlockingQueue<>("./blockingtest", Long.class, 1)) {
			Assertions.assertNull(queue.poll());

			Assertions.assertTrue(queue.offer(1L));
			Assertions.assertEquals(1L, (long) queue.poll());
		}
	}

	@Test
	void testPollLongTimeUnit() {
		try (XodusBlockingQueue<Long> queue = new XodusBlockingQueue<>("./blockingtest", Long.class, 1)) {
			try {
				Assertions.assertNull(queue.poll(2, TimeUnit.SECONDS));
				Assertions.assertTrue(queue.offer(1L));
				Assertions.assertEquals(1L, (long) queue.poll(2, TimeUnit.SECONDS));
				Assertions.assertNull(queue.poll(2, TimeUnit.SECONDS));
			}
			catch (InterruptedException e) {
				Assertions.fail(e);
			}
		}
	}

	@Test
	void testTake() {
		try (XodusBlockingQueue<String> queue = new XodusBlockingQueue<>("./blockingtest", String.class, 1)) {

			CountDownLatch countDown = new CountDownLatch(10);

			Thread producer = new Thread(() -> {
				for (int i = 0; i < 10; i++) {
					try {
						queue.put(String.valueOf(i));
						System.out.println("producer: " + i);
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

			try {
				consumer.start();
				producer.start();
				countDown.await();
			}
			catch (InterruptedException e) {
				Assertions.fail(e);
			}
		}
	}

	@Test
	void testRemoveObject() {
		try (XodusBlockingQueue<String> queue = new XodusBlockingQueue<>("./blockingtest", String.class, 1)) {

			CountDownLatch countDown = new CountDownLatch(1);

			Thread producer = new Thread(() -> {
				try {
					queue.put("one");
					countDown.countDown();
					queue.put("two");
				}
				catch (InterruptedException e) {
					Assertions.fail(e);
				}
			});

			try {
				producer.start();
				countDown.await();
				queue.remove("one");

				Assertions.assertEquals("two", queue.take());
				Assertions.assertEquals(0, queue.size());
			}
			catch (InterruptedException e) {
				Assertions.fail(e);
			}
		}
	}

	@Test
	void testRemoveAllCollectionOfQ() {
		try (XodusBlockingQueue<String> queue = new XodusBlockingQueue<>("./blockingtest", String.class, 3)) {

			CountDownLatch countDown = new CountDownLatch(1);

			Thread producer = new Thread(() -> {
				try {
					queue.put("one");
					queue.put("two");
					queue.put("three");
					countDown.countDown();
					queue.put("four");
				}
				catch (InterruptedException e) {
					Assertions.fail(e);
				}
			});

			try {
				producer.start();
				countDown.await();
				queue.removeAll(Arrays.asList("one", "two"));

				Assertions.assertEquals("three", queue.take());
				Assertions.assertEquals("four", queue.take());
				Assertions.assertEquals(0, queue.size());
			}
			catch (InterruptedException e) {
				Assertions.fail(e);
			}
		}
	}

	@Test
	void testRetainAllCollectionOfQ() {

		try (XodusBlockingQueue<String> queue = new XodusBlockingQueue<>("./blockingtest", String.class, 3)) {

			CountDownLatch countDown = new CountDownLatch(1);

			Thread producer = new Thread(() -> {
				try {
					queue.put("one");
					queue.put("two");
					queue.put("three");
					countDown.countDown();
					queue.put("four");
				}
				catch (InterruptedException e) {
					Assertions.fail(e);
				}
			});

			try {
				producer.start();
				countDown.await();
				queue.retainAll(Arrays.asList("three"));

				Assertions.assertEquals("three", queue.take());
				Assertions.assertEquals("four", queue.take());
				Assertions.assertEquals(0, queue.size());
			}
			catch (InterruptedException e) {
				Assertions.fail(e);
			}
		}

	}

	@Test
	void testDrainToCollectionOfQsuperTInt() {

		try (XodusBlockingQueue<Long> queue = new XodusBlockingQueue<>("./blockingtest", Long.class, 3)) {
			Assertions.assertTrue(queue.offer(11L));
			Assertions.assertTrue(queue.offer(22L));
			Assertions.assertTrue(queue.offer(33L));
			Assertions.assertFalse(queue.offer(44L));

			List<Long> c = new ArrayList<>();
			queue.drainTo(c, 1);

			Assertions.assertEquals(1, c.size());
			Assertions.assertEquals(11L, (long) c.get(0));

			Assertions.assertEquals(2, queue.size());

			c.clear();
			queue.drainTo(c, 2);
			Assertions.assertEquals(2, c.size());
			Assertions.assertEquals(22L, (long) c.get(0));
			Assertions.assertEquals(33L, (long) c.get(1));

			c.clear();
			queue.drainTo(c, 2);
			Assertions.assertTrue(c.isEmpty());
		}

	}

	@Test
	void testDrainToCollectionOfQsuperT() {
		try (XodusBlockingQueue<Long> queue = new XodusBlockingQueue<>("./blockingtest", Long.class, 3)) {
			Assertions.assertTrue(queue.offer(11L));
			Assertions.assertTrue(queue.offer(22L));
			Assertions.assertTrue(queue.offer(33L));
			Assertions.assertFalse(queue.offer(44L));

			List<Long> c = new ArrayList<>();
			queue.drainTo(c);

			Assertions.assertEquals(3, c.size());
			Assertions.assertEquals(11L, (long) c.get(0));
			Assertions.assertEquals(22L, (long) c.get(1));
			Assertions.assertEquals(33L, (long) c.get(2));
			Assertions.assertEquals(0, queue.size());
		}
	}

	@Test
	void testPut() {
		try (XodusBlockingQueue<String> queue = new XodusBlockingQueue<>("./blockingtest", String.class, 1)) {

			CountDownLatch countDown = new CountDownLatch(1);

			Thread producer = new Thread(() -> {
				try {
					queue.put("first");
					countDown.countDown();
					queue.put("second");
				}
				catch (InterruptedException e) {
					Assertions.fail(e);
				}
			});

			try {
				producer.start();
				countDown.await();
				Assertions.assertEquals("first", queue.take());
				TimeUnit.MILLISECONDS.sleep(100);
				Assertions.assertEquals("second", queue.take());
			}
			catch (InterruptedException e) {
				Assertions.fail(e);
			}
		}
	}

	@Test
	void testPeek() {
		try (XodusBlockingQueue<String> queue = new XodusBlockingQueue<>("./blockingtest", String.class, 1)) {
			Assertions.assertNull(queue.peek());
			queue.offer("one");
			Assertions.assertEquals("one", queue.peek());
			Assertions.assertEquals(1, queue.size());
		}
	}

	@Test
	void testElement() {
		try (XodusBlockingQueue<String> queue = new XodusBlockingQueue<>("./blockingtest", String.class, 1)) {
			Assertions.assertThrows(NoSuchElementException.class, () -> queue.element());
			queue.offer("one");
			Assertions.assertEquals("one", queue.element());
			Assertions.assertEquals(1, queue.size());
		}
	}

	@Test
	void testIsEmpty() {
		try (XodusBlockingQueue<String> queue = new XodusBlockingQueue<>("./blockingtest", String.class, 1)) {
			Assertions.assertTrue(queue.isEmpty());
			queue.offer("one");
			Assertions.assertFalse(queue.isEmpty());
			queue.poll();
			Assertions.assertTrue(queue.isEmpty());
		}
	}

	@Test
	void testContains() {
		try (XodusBlockingQueue<String> queue = new XodusBlockingQueue<>("./blockingtest", String.class, 3)) {
			Assertions.assertFalse(queue.contains("one"));
			queue.offer("one");
			queue.offer("two");
			Assertions.assertTrue(queue.contains("one"));
			Assertions.assertTrue(queue.contains("two"));
			Assertions.assertFalse(queue.contains("three"));
		}
	}

	@Test
	void testToArray() {
		try (XodusBlockingQueue<String> queue = new XodusBlockingQueue<>("./blockingtest", String.class, 3)) {
			queue.offer("one");
			queue.offer("two");
			Object[] arr = queue.toArray();
			Assertions.assertEquals(2, arr.length);
			Assertions.assertEquals("one", arr[0]);
			Assertions.assertEquals("two", arr[1]);
		}
	}

	@Test
	void testToArrayTArray() {
		try (XodusBlockingQueue<String> queue = new XodusBlockingQueue<>("./blockingtest", String.class, 3)) {
			queue.offer("one");
			queue.offer("two");
			String[] arr = queue.toArray(new String[0]);
			Assertions.assertEquals(2, arr.length);
			Assertions.assertEquals("one", arr[0]);
			Assertions.assertEquals("two", arr[1]);
		}
	}

	@Test
	void testIterator() {
		try (XodusBlockingQueue<String> queue = new XodusBlockingQueue<>("./blockingtest", String.class, 3)) {
			queue.offer("one");
			queue.offer("two");
			var iterator = queue.iterator();
			Assertions.assertTrue(iterator.hasNext());
			Assertions.assertEquals("one", iterator.next());
			Assertions.assertTrue(iterator.hasNext());
			Assertions.assertEquals("two", iterator.next());
			Assertions.assertFalse(iterator.hasNext());
		}
	}

	@Test
	void testNullElement() {
		try (XodusBlockingQueue<String> queue = new XodusBlockingQueue<>("./blockingtest", String.class, 1)) {
			Assertions.assertThrows(NullPointerException.class, () -> queue.add(null));
			Assertions.assertThrows(NullPointerException.class, () -> queue.offer(null));
			Assertions.assertThrows(NullPointerException.class, () -> queue.put(null));
		}
	}

	@Test
	void testInterruptedException() {
		try (XodusBlockingQueue<String> queue = new XodusBlockingQueue<>("./blockingtest", String.class, 1)) {
			queue.offer("one"); // fill to capacity

			Thread consumer = new Thread(() -> {
				try {
					queue.take();
				}
				catch (InterruptedException e) {
					// Expected
				}
			});

			consumer.start();
			TimeUnit.MILLISECONDS.sleep(100);
			consumer.interrupt();
		}
		catch (InterruptedException e) {
			Assertions.fail(e);
		}
	}

}
