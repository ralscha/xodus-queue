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

import java.util.Arrays;
import java.util.Collections;
import java.util.NoSuchElementException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class XodusQueueTest {

	@BeforeEach
	public void deleteAll() {
		TestUtil.deleteDirectory("./test");
	}

	@AfterAll
	public static void deleteAllEnd() {
		TestUtil.deleteDirectory("./test");
	}

	@Test
	void testShutdown() {
		try (XodusQueue<String> queue = new XodusQueue<>("./test", String.class)) {
			queue.add("one");
			queue.add("two");
			queue.add("three");
			Assertions.assertEquals(3, queue.size());
		}

		try (XodusQueue<String> queue = new XodusQueue<>("./test", String.class)) {
			queue.add("four");
			Assertions.assertEquals(4, queue.size());
		}

		try (XodusQueue<String> queue = new XodusQueue<>("./test", String.class)) {
			Assertions.assertEquals("one", queue.remove());
			Assertions.assertEquals(3, queue.size());
		}
	}

	@Test
	void testSize() {
		try (XodusQueue<String> queue = new XodusQueue<>("./test", String.class)) {
			Assertions.assertEquals(0, queue.size());
			queue.add("one");
			Assertions.assertEquals(1, queue.size());
			queue.add("two");
			Assertions.assertEquals(2, queue.size());
			queue.add("three");
			Assertions.assertEquals(3, queue.size());
			String head = queue.poll();
			Assertions.assertEquals("one", head);
			Assertions.assertEquals(2, queue.size());
			head = queue.poll();
			Assertions.assertEquals("two", head);
			Assertions.assertEquals(1, queue.size());
			head = queue.poll();
			Assertions.assertEquals("three", head);
			Assertions.assertEquals(0, queue.size());

			queue.add("one");
			Assertions.assertEquals(1, queue.size());
			queue.add("two");
			queue.clear();
			Assertions.assertEquals(0, queue.size());
		}
	}

	@Test
	void testSizeLong() {
		try (XodusQueue<String> queue = new XodusQueue<>("./test", String.class)) {
			Assertions.assertEquals(0L, queue.sizeLong());
			queue.add("one");
			Assertions.assertEquals(1L, queue.sizeLong());
			queue.add("two");
			Assertions.assertEquals(2L, queue.sizeLong());
			queue.add("three");
			Assertions.assertEquals(3L, queue.sizeLong());
			String head = queue.poll();
			Assertions.assertEquals("one", head);
			Assertions.assertEquals(2L, queue.sizeLong());
			head = queue.poll();
			Assertions.assertEquals("two", head);
			Assertions.assertEquals(1L, queue.sizeLong());
			head = queue.poll();
			Assertions.assertEquals("three", head);
			Assertions.assertEquals(0L, queue.sizeLong());

			queue.add("one");
			Assertions.assertEquals(1L, queue.sizeLong());
			queue.add("two");
			queue.clear();
			Assertions.assertEquals(0L, queue.sizeLong());
		}
	}

	@Test
	void testIsEmpty() {
		try (XodusQueue<String> queue = new XodusQueue<>("./test", String.class)) {
			Assertions.assertTrue(queue.isEmpty());
			queue.add("one");
			Assertions.assertFalse(queue.isEmpty());
			queue.add("two");
			Assertions.assertFalse(queue.isEmpty());

			String head = queue.poll();
			Assertions.assertEquals("one", head);
			Assertions.assertFalse(queue.isEmpty());
			head = queue.poll();
			Assertions.assertEquals("two", head);
			Assertions.assertTrue(queue.isEmpty());

			queue.add("one");
			Assertions.assertFalse(queue.isEmpty());
			queue.add("two");
			queue.clear();
			Assertions.assertTrue(queue.isEmpty());
		}
	}

	@Test
	void testClear() {
		try (XodusQueue<String> queue = new XodusQueue<>("./test", String.class)) {
			queue.add("one");
			queue.add("two");
			queue.clear();
			Assertions.assertTrue(queue.isEmpty());
			Assertions.assertEquals(0, queue.size());
			Assertions.assertEquals(0L, queue.sizeLong());
			String head = queue.poll();
			Assertions.assertNull(head);

			queue.clear();
			Assertions.assertTrue(queue.isEmpty());
			Assertions.assertEquals(0, queue.size());
			Assertions.assertEquals(0L, queue.sizeLong());
			head = queue.poll();
			Assertions.assertNull(head);
		}
	}

	@Test
	void testAdd() {
		try (XodusQueue<Integer> queue = new XodusQueue<>("./test", Integer.class)) {
			Assertions.assertTrue(queue.add(1));
			Assertions.assertTrue(queue.add(2));
			Assertions.assertTrue(queue.add(3));
			Assertions.assertEquals(3, queue.size());
			Assertions.assertThrows(NullPointerException.class, () -> queue.add(null));
		}
	}

	@Test
	void testAddAllCollectionOfQextendsT() {
		try (XodusQueue<Integer> queue = new XodusQueue<>("./test", Integer.class)) {
			Assertions.assertThrows(NullPointerException.class, null);

			Assertions.assertTrue(queue.addAll(Arrays.asList(1, 2, 3, 4, 5)));
			Assertions.assertEquals(5, queue.size());

			Assertions.assertEquals(1, (int) queue.poll());
			Assertions.assertEquals(2, (int) queue.poll());
			Assertions.assertEquals(3, (int) queue.poll());

			Assertions.assertTrue(queue.addAll(Arrays.asList(6, 7, 8)));
			Assertions.assertEquals(5, queue.size());

			Assertions.assertEquals(4, (int) queue.poll());
			Assertions.assertEquals(5, (int) queue.poll());
			Assertions.assertEquals(3, queue.size());

			Assertions.assertEquals(6, (int) queue.poll());
			Assertions.assertEquals(7, (int) queue.poll());
			Assertions.assertEquals(8, (int) queue.poll());
			Assertions.assertEquals(0, queue.size());

			Assertions.assertTrue(queue.addAll(Arrays.asList(9, 10, 11)));
			Assertions.assertEquals(3, queue.size());

			Assertions.assertEquals(9, (int) queue.poll());
			Assertions.assertEquals(10, (int) queue.poll());
			Assertions.assertEquals(11, (int) queue.poll());
			Assertions.assertEquals(0, queue.size());
		}
	}

	@Test
	void testOffer() {
		try (XodusQueue<Integer> queue = new XodusQueue<>("./test", Integer.class)) {
			Assertions.assertTrue(queue.offer(1));
			Assertions.assertTrue(queue.offer(2));
			Assertions.assertTrue(queue.offer(3));
			Assertions.assertEquals(3, queue.size());
			Assertions.assertThrows(NullPointerException.class, () -> queue.offer(null));

			Assertions.assertEquals(1, (int) queue.poll());
			Assertions.assertTrue(queue.offer(4));

			Assertions.assertEquals(2, (int) queue.poll());
			Assertions.assertTrue(queue.offer(5));

			Assertions.assertEquals(3, (int) queue.poll());
			Assertions.assertTrue(queue.offer(6));

			Assertions.assertEquals(4, (int) queue.poll());
			Assertions.assertEquals(5, (int) queue.poll());
			Assertions.assertEquals(6, (int) queue.poll());

			Assertions.assertEquals(0, queue.size());
			Assertions.assertTrue(queue.offer(7));
			Assertions.assertEquals(7, (int) queue.poll());

			Assertions.assertEquals(0, queue.size());
			Assertions.assertTrue(queue.offer(8));
			Assertions.assertEquals(8, (int) queue.poll());
		}
	}

	@Test
	void testPoll() {
		try (XodusQueue<TestPojo> queue = new XodusQueue<>("./test", TestPojo.class)) {
			TestPojo one = new TestPojo(1, "John");
			queue.offer(one);
			TestPojo two = new TestPojo(2, "Anna");
			queue.offer(two);
			TestPojo three = new TestPojo(3, "Li");
			queue.offer(three);
			Assertions.assertEquals(3, queue.size());

			Assertions.assertEquals(one, queue.poll());
			Assertions.assertEquals(two, queue.poll());
			Assertions.assertEquals(three, queue.poll());
			Assertions.assertNull(queue.poll());
			Assertions.assertNull(queue.poll());
		}
	}

	@Test
	void testRemove() {
		try (XodusQueue<TestPojo> queue = new XodusQueue<>("./test", TestPojo.class)) {
			TestPojo one = new TestPojo(1, "John");
			queue.offer(one);
			TestPojo two = new TestPojo(2, "Anna");
			queue.offer(two);
			TestPojo three = new TestPojo(3, "Li");
			queue.offer(three);
			Assertions.assertEquals(3, queue.size());

			Assertions.assertEquals(one, queue.remove());
			Assertions.assertEquals(two, queue.remove());
			Assertions.assertEquals(three, queue.remove());
			Assertions.assertEquals(0, queue.size());
			Assertions.assertThrows(NoSuchElementException.class, () -> queue.remove());
			Assertions.assertThrows(NoSuchElementException.class, () -> queue.remove());
		}
	}

	@Test
	void testRemoveObject() {
		try (XodusQueue<TestPojo> queue = new XodusQueue<>("./test", TestPojo.class)) {
			TestPojo one = new TestPojo(1, "John");
			Assertions.assertFalse(queue.remove(one));

			queue.offer(one);
			TestPojo two = new TestPojo(2, "Anna");
			queue.offer(two);
			TestPojo three = new TestPojo(3, "Li");
			queue.offer(three);
			Assertions.assertEquals(3, queue.size());

			Assertions.assertFalse(queue.remove(new TestPojo(4, "Dean")));
			Assertions.assertTrue(queue.remove(one));
			Assertions.assertEquals(2, queue.size());

			Assertions.assertEquals(two, queue.remove());
			Assertions.assertEquals(three, queue.remove());
			Assertions.assertEquals(0, queue.size());

			Assertions.assertFalse(queue.remove(two));
		}
	}

	@Test
	void testRemoveAllCollectionOfQ() {
		try (XodusQueue<TestPojo> queue = new XodusQueue<>("./test", TestPojo.class)) {
			TestPojo one = new TestPojo(1, "John");
			Assertions.assertFalse(queue.removeAll(Collections.emptyList()));
			Assertions.assertFalse(queue.removeAll(Collections.singleton(one)));

			queue.offer(one);
			TestPojo two = new TestPojo(2, "Anna");
			queue.offer(two);
			TestPojo three = new TestPojo(3, "Li");
			queue.offer(three);
			Assertions.assertEquals(3, queue.size());

			Assertions.assertFalse(
					queue.removeAll(Collections.singleton(new TestPojo(4, "Dean"))));
			Assertions.assertTrue(queue.removeAll(Collections.singleton(one)));
			Assertions.assertEquals(2, queue.size());

			Assertions.assertEquals(two, queue.remove());
			Assertions.assertEquals(three, queue.remove());
			Assertions.assertEquals(0, queue.size());

			queue.offer(one);
			queue.offer(two);
			queue.offer(three);
			Assertions.assertEquals(3, queue.size());
			Assertions.assertTrue(queue
					.removeAll(Arrays.asList(one, two, three, new TestPojo(4, "Dean"))));
			Assertions.assertEquals(0, queue.size());
			Assertions.assertTrue(queue.isEmpty());
		}
	}

	@Test
	void testRetainAllCollectionOfQ() {
		try (XodusQueue<TestPojo> queue = new XodusQueue<>("./test", TestPojo.class)) {
			TestPojo one = new TestPojo(1, "John");
			Assertions.assertFalse(queue.retainAll(Collections.emptyList()));
			Assertions.assertFalse(queue.retainAll(Collections.singleton(one)));

			queue.offer(one);
			TestPojo two = new TestPojo(2, "Anna");
			queue.offer(two);
			TestPojo three = new TestPojo(3, "Li");
			queue.offer(three);
			Assertions.assertEquals(3, queue.size());

			Assertions.assertTrue(
					queue.retainAll(Collections.singleton(new TestPojo(4, "Dean"))));
			Assertions.assertEquals(0, queue.size());

			queue.offer(one);
			queue.offer(two);
			queue.offer(three);
			Assertions.assertTrue(queue.retainAll(Collections.singleton(one)));
			Assertions.assertEquals(1, queue.size());
			Assertions.assertEquals(one, queue.remove());
			Assertions.assertEquals(0, queue.size());

			queue.offer(one);
			queue.offer(two);
			queue.offer(three);
			Assertions.assertEquals(3, queue.size());
			Assertions.assertFalse(queue
					.retainAll(Arrays.asList(one, two, three, new TestPojo(4, "Dean"))));
			Assertions.assertEquals(3, queue.size());
		}
	}

	@Test
	void testPeek() {
		try (XodusQueue<TestPojo> queue = new XodusQueue<>("./test", TestPojo.class)) {
			TestPojo one = new TestPojo(1, "John");
			queue.offer(one);
			TestPojo two = new TestPojo(2, "Anna");
			queue.offer(two);
			TestPojo three = new TestPojo(3, "Li");
			queue.offer(three);
			Assertions.assertEquals(3, queue.size());

			Assertions.assertEquals(one, queue.peek());
			Assertions.assertEquals(one, queue.peek());
			Assertions.assertEquals(one, queue.poll());
			Assertions.assertEquals(two, queue.peek());
			Assertions.assertEquals(two, queue.poll());
			Assertions.assertEquals(three, queue.poll());
			Assertions.assertNull(queue.peek());
			Assertions.assertNull(queue.peek());
		}
	}

	@Test
	void testElement() {
		try (XodusQueue<TestPojo> queue = new XodusQueue<>("./test", TestPojo.class)) {
			TestPojo one = new TestPojo(1, "John");
			queue.offer(one);
			TestPojo two = new TestPojo(2, "Anna");
			queue.offer(two);
			TestPojo three = new TestPojo(3, "Li");
			queue.offer(three);
			Assertions.assertEquals(3, queue.size());

			Assertions.assertEquals(one, queue.element());
			Assertions.assertEquals(one, queue.element());
			Assertions.assertEquals(one, queue.poll());
			Assertions.assertEquals(two, queue.element());
			Assertions.assertEquals(two, queue.poll());
			Assertions.assertEquals(three, queue.poll());
			Assertions.assertEquals(0, queue.size());
			Assertions.assertThrows(NoSuchElementException.class, () -> queue.element());
			Assertions.assertThrows(NoSuchElementException.class, () -> queue.element());
		}
	}

	@Test
	void testContainsObject() {
		try (XodusQueue<TestPojo> queue = new XodusQueue<>("./test", TestPojo.class)) {
			TestPojo one = new TestPojo(1, "John");
			queue.offer(one);
			TestPojo two = new TestPojo(2, "Anna");
			queue.offer(two);
			TestPojo three = new TestPojo(3, "Li");
			queue.offer(three);
			Assertions.assertEquals(3, queue.size());

			Assertions.assertTrue(queue.contains(one));
			Assertions.assertTrue(queue.contains(two));
			Assertions.assertTrue(queue.contains(three));
			Assertions.assertFalse(queue.contains(new TestPojo(4, "Dean")));
		}
	}

	@Test
	void testContainsAllCollectionOfQ() {
		try (XodusQueue<TestPojo> queue = new XodusQueue<>("./test", TestPojo.class)) {
			TestPojo one = new TestPojo(1, "John");
			queue.offer(one);
			TestPojo two = new TestPojo(2, "Anna");
			queue.offer(two);
			TestPojo three = new TestPojo(3, "Li");
			queue.offer(three);
			Assertions.assertEquals(3, queue.size());

			Assertions.assertTrue(queue.containsAll(Arrays.asList(one, two, three)));
			Assertions.assertTrue(queue.containsAll(Arrays.asList(one, two)));
			Assertions.assertTrue(queue.containsAll(Arrays.asList(one)));
			Assertions.assertTrue(queue.containsAll(Collections.emptyList()));
			Assertions.assertFalse(
					queue.containsAll(Arrays.asList(one, two, new TestPojo(4, "Adam"))));

			queue.poll();
			Assertions.assertFalse(queue.containsAll(Arrays.asList(one, two, three)));

			queue.clear();
			Assertions.assertEquals(0, queue.size());
			Assertions.assertFalse(queue.containsAll(Arrays.asList(one, two, three)));

		}
	}

	@Test
	void testToArray() {
		try (XodusQueue<String> queue = new XodusQueue<>("./test", String.class)) {
			queue.add("one");
			queue.add("two");
			queue.add("three");
			Object[] result = queue.toArray();
			Assertions.assertEquals(3, queue.size());
			Assertions.assertArrayEquals(new String[] { "one", "two", "three" }, result);

			queue.clear();
			result = queue.toArray();
			Assertions.assertArrayEquals(new String[0], result);
		}
	}

	@Test
	void testToArrayTArray() {
		try (XodusQueue<String> queue = new XodusQueue<>("./test", String.class)) {
			queue.add("one");
			queue.add("two");
			queue.add("three");
			String[] result = queue.toArray(new String[1]);
			Assertions.assertEquals(3, queue.size());
			Assertions.assertArrayEquals(new String[] { "one", "two", "three" }, result);

			result = queue.toArray(new String[3]);
			Assertions.assertArrayEquals(new String[] { "one", "two", "three" }, result);

			result = queue.toArray(new String[4]);
			Assertions.assertArrayEquals(new String[] { "one", "two", "three", null },
					result);

			queue.clear();
			result = queue.toArray(new String[5]);
			Assertions.assertArrayEquals(new String[5], result);
		}
	}

}
