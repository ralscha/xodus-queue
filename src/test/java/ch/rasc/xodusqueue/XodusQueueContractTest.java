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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import ch.rasc.xodusqueue.serializer.DefaultXodusQueueSerializer;
import ch.rasc.xodusqueue.serializer.XodusQueueSerializer;

class XodusQueueContractTest {

	@TempDir
	Path tempDir;

	private String dbDir(String name) {
		return this.tempDir.resolve(name).toString();
	}

	@Test
	void constructorRejectsInvalidSerializerArgumentsBeforeOpeningDatabase() {
		Path classDatabase = this.tempDir.resolve("null-class");
		Assertions.assertThrows(NullPointerException.class,
				() -> new XodusQueue<>(classDatabase.toString(), (Class<String>) null));
		Assertions.assertFalse(Files.exists(classDatabase));

		Path serializerDatabase = this.tempDir.resolve("null-serializer");
		Assertions.assertThrows(NullPointerException.class,
				() -> new XodusQueue<>(serializerDatabase.toString(), (XodusQueueSerializer<String>) null));
		Assertions.assertFalse(Files.exists(serializerDatabase));

		Assertions.assertThrows(NullPointerException.class, () -> new DefaultXodusQueueSerializer<String>(null));
	}

	@Test
	void removeIfRemovesMatchingElementsInQueueOrder() {
		try (XodusQueue<Integer> queue = new XodusQueue<>(dbDir("remove-if"), Integer.class)) {
			queue.addAll(List.of(1, 2, 3, 4));

			Assertions.assertTrue(queue.removeIf(value -> value % 2 == 0));
			Assertions.assertArrayEquals(new Integer[] { 1, 3 }, queue.toArray(Integer[]::new));
			Assertions.assertFalse(queue.removeIf(value -> value > 10));
			Assertions.assertThrows(NullPointerException.class, () -> queue.removeIf(null));
		}
	}

	@Test
	void bulkOperationsHandleTheQueueItself() {
		try (XodusQueue<Integer> queue = new XodusQueue<>(dbDir("self-bulk-operations"), Integer.class)) {
			Assertions.assertFalse(queue.removeAll(queue));
			queue.addAll(List.of(1, 2));

			Assertions.assertTrue(queue.containsAll(queue));
			Assertions.assertFalse(queue.retainAll(queue));
			Assertions.assertArrayEquals(new Integer[] { 1, 2 }, queue.toArray(Integer[]::new));
			Assertions.assertTrue(queue.removeAll(queue));
			Assertions.assertTrue(queue.isEmpty());
		}
	}

	@Test
	void blockingQueueRemoveIfReleasesCapacity() {
		try (XodusBlockingQueue<Integer> queue = new XodusBlockingQueue<>(dbDir("blocking-remove-if"), Integer.class,
				2)) {
			queue.addAll(List.of(1, 2));

			Assertions.assertTrue(queue.removeIf(value -> value == 1));
			Assertions.assertEquals(1, queue.remainingCapacity());
			Assertions.assertTrue(queue.offer(3));
			Assertions.assertArrayEquals(new Integer[] { 2, 3 }, queue.toArray(Integer[]::new));
		}
	}

	@Test
	void remainingCapacityNeverBecomesNegativeWhenReopenedWithSmallerCapacity() {
		String databaseDir = dbDir("smaller-capacity");
		try (XodusQueue<String> queue = new XodusQueue<>(databaseDir, String.class)) {
			queue.addAll(List.of("one", "two", "three"));
		}

		try (XodusBlockingQueue<String> queue = new XodusBlockingQueue<>(databaseDir, String.class, 2)) {
			Assertions.assertEquals(0, queue.remainingCapacity());
			Assertions.assertFalse(queue.offer("four"));
			Assertions.assertEquals("one", queue.poll());
			Assertions.assertEquals(0, queue.remainingCapacity());
			Assertions.assertEquals("two", queue.poll());
			Assertions.assertEquals(1, queue.remainingCapacity());
		}
	}

}
