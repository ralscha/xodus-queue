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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AdditionalTests {

	@BeforeEach
	public void deleteAll() {
		TestUtil.deleteDirectory("./testedge");
		TestUtil.deleteDirectory("./testedge2");
		TestUtil.deleteDirectory("./testedge3");
		TestUtil.deleteDirectory("./testedge4");
		TestUtil.deleteDirectory("./blockingtest_small");
		TestUtil.deleteDirectory("./testedge5");
	}

	@AfterAll
	public static void deleteAllEnd() {
		TestUtil.deleteDirectory("./testedge");
		TestUtil.deleteDirectory("./testedge2");
		TestUtil.deleteDirectory("./testedge3");
		TestUtil.deleteDirectory("./testedge4");
		TestUtil.deleteDirectory("./blockingtest_small");
		TestUtil.deleteDirectory("./testedge5");
	}

	@Test
	void testRetainAllWithEmptyCollectionClearsWhenNotEmpty() {
		try (XodusQueue<String> queue = new XodusQueue<>("./testedge", String.class)) {
			queue.add("a");
			queue.add("b");
			// per implementation, retainAll(empty) should clear and return true
			Assertions.assertTrue(queue.retainAll(Collections.emptyList()));
			Assertions.assertTrue(queue.isEmpty());
		}
	}

	@Test
	void testIteratorIsSnapshot() {
		try (XodusQueue<String> queue = new XodusQueue<>("./testedge2", String.class)) {
			queue.add("one");
			queue.add("two");

			Iterator<String> it = queue.iterator();

			// modify after creating iterator
			queue.add("three");

			List<String> items = new ArrayList<>();
			it.forEachRemaining(items::add);

			Assertions.assertEquals(Arrays.asList("one", "two"), items);
			// queue itself still contains all three
			Assertions.assertEquals(3, queue.size());
		}
	}

	@Test
	void testDrainToSelfThrows() {
		try (XodusBlockingQueue<String> queue = new XodusBlockingQueue<>("./testedge3", String.class, 10L)) {
			queue.add("x");
			Assertions.assertThrows(IllegalArgumentException.class, () -> queue.drainTo(queue));
		}
	}

	@Test
	void testAddAllSelfThrows() {
		try (XodusQueue<String> queue = new XodusQueue<>("./testedge4", String.class)) {
			Assertions.assertThrows(IllegalArgumentException.class, () -> queue.addAll(queue));
		}
	}

	@Test
	void testBlockingQueueCapacityAndOfferTimeout() throws Exception {
		try (XodusBlockingQueue<Integer> queue = new XodusBlockingQueue<>("./blockingtest_small", Integer.class, 2L)) {
			Assertions.assertTrue(queue.offer(1));
			Assertions.assertTrue(queue.offer(2));

			// immediate offer should fail when full
			Assertions.assertFalse(queue.offer(3));

			// free space in another thread after a short delay
			Thread t = new Thread(() -> {
				try {
					Thread.sleep(200);
					queue.poll();
				}
				catch (InterruptedException e) {
					// ignore
				}
			});
			t.start();

			// offer with timeout should wait and succeed once space is available
			boolean offered = queue.offer(4, 1, TimeUnit.SECONDS);
			Assertions.assertTrue(offered);
		}
	}

	@Test
	void testRemoveAllWithEmptyCollectionReturnsFalseWhenNotEmpty() {
		try (XodusQueue<String> queue = new XodusQueue<>("./testedge5", String.class)) {
			queue.add("a");
			Assertions.assertFalse(queue.removeAll(Collections.emptyList()));
			Assertions.assertEquals(1, queue.size());
		}
	}

}
