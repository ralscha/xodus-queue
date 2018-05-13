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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import ch.rasc.xodusqueue.serializer.IntegerXodusQueueSerializer;
import ch.rasc.xodusqueue.serializer.StringXodusQueueSerializer;

public class XodusQueue2Test {

	@BeforeEach
	public void deleteAll() {
		TestUtil.deleteDirectory("./queue");
	}

	@AfterAll
	public static void deleteAllEnd() {
		TestUtil.deleteDirectory("./queue");
	}

	@Test
	public void testPush() throws Throwable {
		try (XodusQueue<String> queue = new XodusQueue<>("./queue", String.class)) {
			queue.add("1");
			queue.add("2");
			String head = queue.poll();
			Assertions.assertEquals("1", head);
		}
	}

	@Test
	public void testQueueSurviveReopen() throws Throwable {
		try (XodusQueue<String> queue = new XodusQueue<>("./queue",
				new StringXodusQueueSerializer())) {
			queue.add("5");
		}

		try (XodusQueue<String> queue = new XodusQueue<>("./queue",
				new StringXodusQueueSerializer())) {
			String head = queue.poll();
			Assertions.assertEquals("5", head);
		}
	}

	@Test
	public void testQueuePushOrder() throws Throwable {
		try (XodusQueue<Integer> queue = new XodusQueue<>("./queue",
				new IntegerXodusQueueSerializer())) {
			for (int i = 0; i < 300; i++) {
				queue.add(i);
			}

			for (int i = 0; i < 300; i++) {
				int element = queue.poll();
				Assertions.assertEquals(i, element);
			}
		}
	}

	@Test
	public void testMultiThreadedPoll() throws Throwable {
		int threadCount = 20;
		final Set<String> set = ConcurrentHashMap.newKeySet();
		final CountDownLatch startLatch = new CountDownLatch(threadCount);
		final CountDownLatch latch = new CountDownLatch(threadCount);

		try (XodusQueue<String> queue = new XodusQueue<>("./queue",
				new StringXodusQueueSerializer())) {
			for (int i = 0; i < threadCount; i++) {
				queue.add(Integer.toString(i));
			}

			for (int i = 0; i < threadCount; i++) {
				new Thread() {
					@Override
					public void run() {
						try {
							startLatch.countDown();
							startLatch.await();

							String val = queue.poll();
							if (val != null) {
								set.add(val);
							}
							latch.countDown();
						}
						catch (Throwable e) {
							e.printStackTrace();
						}
					}
				}.start();
			}

			latch.await(5, TimeUnit.SECONDS);

			assert set.size() == threadCount;
		}
	}

	@Test
	public void testMultiThreadedPush() throws Throwable {

		int threadCount = 20;
		CountDownLatch startLatch = new CountDownLatch(threadCount);
		CountDownLatch latch = new CountDownLatch(threadCount);

		try (XodusQueue<String> queue = new XodusQueue<>("./queue",
				new StringXodusQueueSerializer())) {
			for (int i = 0; i < threadCount; i++) {
				new Thread(Integer.toString(i)) {
					@Override
					public void run() {
						try {
							startLatch.countDown();
							startLatch.await();

							queue.add(getName());
							latch.countDown();
						}
						catch (Throwable e) {
							e.printStackTrace();
						}
					}
				}.start();
			}

			latch.await(5, TimeUnit.SECONDS);

			assert queue.size() == threadCount;
		}

	}

}