package ch.rasc.xodusqueue;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SerializerTest {

	@BeforeEach
	public void deleteAll() {
		TestUtil.deleteDirectory("./test");
	}

	@AfterAll
	public static void deleteAllEnd() {
		TestUtil.deleteDirectory("./test");
	}

	@Test
	void testBoolean() {
		try (XodusQueue<Boolean> queue = new XodusQueue<>("./test", Boolean.class)) {
			Assertions.assertEquals(0L, queue.sizeLong());
			queue.add(true);
			Assertions.assertEquals(1L, queue.sizeLong());

			queue.add(Boolean.TRUE);
			queue.add(Boolean.FALSE);
			queue.add(Boolean.FALSE);
			queue.offer(true);
			queue.offer(false);

			Assertions.assertEquals(6L, queue.sizeLong());
			Assertions.assertEquals(true, queue.element());
			Assertions.assertEquals(true, queue.peek());
			Assertions.assertEquals(true, queue.poll());
			Assertions.assertEquals(true, queue.remove());
			Assertions.assertEquals(false, queue.remove());
			Assertions.assertEquals(false, queue.remove());
			Assertions.assertEquals(true, queue.poll());
			Assertions.assertEquals(false, queue.poll());
			Assertions.assertEquals(0L, queue.sizeLong());
			Assertions.assertTrue(queue.isEmpty());
		}
	}

	@Test
	void testByte() {
		try (XodusQueue<Byte> queue = new XodusQueue<>("./test", Byte.class)) {
			Assertions.assertEquals(0L, queue.sizeLong());
			queue.add((byte) 0);
			Assertions.assertEquals(1L, queue.sizeLong());

			queue.add((byte) 1);
			queue.add((byte) 2);
			queue.add((byte) 3);
			queue.offer((byte) 4);
			queue.offer((byte) 5);

			Assertions.assertEquals(6L, queue.sizeLong());
			Assertions.assertEquals((byte) 0, (byte) queue.element());
			Assertions.assertEquals((byte) 0, (byte) queue.peek());
			Assertions.assertEquals((byte) 0, (byte) queue.poll());
			Assertions.assertEquals((byte) 1, (byte) queue.remove());
			Assertions.assertEquals((byte) 2, (byte) queue.remove());
			Assertions.assertEquals((byte) 3, (byte) queue.remove());
			Assertions.assertEquals((byte) 4, (byte) queue.poll());
			Assertions.assertEquals((byte) 5, (byte) queue.poll());
			Assertions.assertEquals(0L, queue.sizeLong());
			Assertions.assertTrue(queue.isEmpty());
		}
	}

	@Test
	void testShort() {
		try (XodusQueue<Short> queue = new XodusQueue<>("./test", Short.class)) {
			Assertions.assertEquals(0L, queue.sizeLong());
			queue.add((short) 0);
			Assertions.assertEquals(1L, queue.sizeLong());

			queue.add((short) 1);
			queue.add((short) 2);
			queue.add((short) 3);
			queue.offer((short) 4);
			queue.offer((short) 5);

			Assertions.assertEquals(6L, queue.sizeLong());
			Assertions.assertEquals((short) 0, (short) queue.element());
			Assertions.assertEquals((short) 0, (short) queue.peek());
			Assertions.assertEquals((short) 0, (short) queue.poll());
			Assertions.assertEquals((short) 1, (short) queue.remove());
			Assertions.assertEquals((short) 2, (short) queue.remove());
			Assertions.assertEquals((short) 3, (short) queue.remove());
			Assertions.assertEquals((short) 4, (short) queue.poll());
			Assertions.assertEquals((short) 5, (short) queue.poll());
			Assertions.assertEquals(0L, queue.sizeLong());
			Assertions.assertTrue(queue.isEmpty());
		}
	}

	@Test
	void testInteger() {
		try (XodusQueue<Integer> queue = new XodusQueue<>("./test", Integer.class)) {
			Assertions.assertEquals(0L, queue.sizeLong());
			queue.add(0);
			Assertions.assertEquals(1L, queue.sizeLong());

			queue.add(1);
			queue.add(2);
			queue.add(3);
			queue.offer(4);
			queue.offer(5);

			Assertions.assertEquals(6L, queue.sizeLong());
			Assertions.assertEquals(0, (int) queue.element());
			Assertions.assertEquals(0, (int) queue.peek());
			Assertions.assertEquals(0, (int) queue.poll());
			Assertions.assertEquals(1, (int) queue.remove());
			Assertions.assertEquals(2, (int) queue.remove());
			Assertions.assertEquals(3, (int) queue.remove());
			Assertions.assertEquals(4, (int) queue.poll());
			Assertions.assertEquals(5, (int) queue.poll());
			Assertions.assertEquals(0L, queue.sizeLong());
			Assertions.assertTrue(queue.isEmpty());
		}
	}

	@Test
	void testLong() {
		try (XodusQueue<Long> queue = new XodusQueue<>("./test", Long.class)) {
			Assertions.assertEquals(0L, queue.sizeLong());
			queue.add(0L);
			Assertions.assertEquals(1L, queue.sizeLong());

			queue.add(1L);
			queue.add(2L);
			queue.add(3L);
			queue.offer(4L);
			queue.offer(5L);

			Assertions.assertEquals(6L, queue.sizeLong());
			Assertions.assertEquals(0L, (long) queue.element());
			Assertions.assertEquals(0L, (long) queue.peek());
			Assertions.assertEquals(0L, (long) queue.poll());
			Assertions.assertEquals(1L, (long) queue.remove());
			Assertions.assertEquals(2L, (long) queue.remove());
			Assertions.assertEquals(3L, (long) queue.remove());
			Assertions.assertEquals(4L, (long) queue.poll());
			Assertions.assertEquals(5L, (long) queue.poll());
			Assertions.assertEquals(0L, queue.sizeLong());
			Assertions.assertTrue(queue.isEmpty());
		}
	}

	@Test
	void testFloat() {
		try (XodusQueue<Float> queue = new XodusQueue<>("./test", Float.class)) {
			Assertions.assertEquals(0L, queue.sizeLong());
			queue.add(0.0f);
			Assertions.assertEquals(1L, queue.sizeLong());

			queue.add(1.1f);
			queue.add(2.2f);
			queue.add(3.3f);
			queue.offer(4.4f);
			queue.offer(5.5f);

			Assertions.assertEquals(6L, queue.sizeLong());
			Assertions.assertEquals(0.0f, (float) queue.element());
			Assertions.assertEquals(0.0f, (float) queue.peek());
			Assertions.assertEquals(0.0f, (float) queue.poll());
			Assertions.assertEquals(1.1f, (float) queue.remove());
			Assertions.assertEquals(2.2f, (float) queue.remove());
			Assertions.assertEquals(3.3f, (float) queue.remove());
			Assertions.assertEquals(4.4f, (float) queue.poll());
			Assertions.assertEquals(5.5f, (float) queue.poll());
			Assertions.assertEquals(0L, queue.sizeLong());
			Assertions.assertTrue(queue.isEmpty());
		}
	}

	@Test
	void testDouble() {
		try (XodusQueue<Double> queue = new XodusQueue<>("./test", Double.class)) {
			Assertions.assertEquals(0L, queue.sizeLong());
			queue.add(0.0);
			Assertions.assertEquals(1L, queue.sizeLong());

			queue.add(1.1);
			queue.add(2.2);
			queue.add(3.3);
			queue.offer(4.4);
			queue.offer(5.5);

			Assertions.assertEquals(6L, queue.sizeLong());
			Assertions.assertEquals(0.0, (double) queue.element());
			Assertions.assertEquals(0.0, (double) queue.peek());
			Assertions.assertEquals(0.0, (double) queue.poll());
			Assertions.assertEquals(1.1, (double) queue.remove());
			Assertions.assertEquals(2.2, (double) queue.remove());
			Assertions.assertEquals(3.3, (double) queue.remove());
			Assertions.assertEquals(4.4, (double) queue.poll());
			Assertions.assertEquals(5.5, (double) queue.poll());
			Assertions.assertEquals(0L, queue.sizeLong());
			Assertions.assertTrue(queue.isEmpty());
		}
	}

	@Test
	void testString() {
		try (XodusQueue<String> queue = new XodusQueue<>("./test", String.class)) {
			Assertions.assertEquals(0L, queue.sizeLong());
			queue.add("zero");
			Assertions.assertEquals(1L, queue.sizeLong());

			queue.add("one");
			queue.add("two");
			queue.add("three");
			queue.offer("four");
			queue.offer("five");

			Assertions.assertEquals(6L, queue.sizeLong());
			Assertions.assertEquals("zero", queue.element());
			Assertions.assertEquals("zero", queue.peek());
			Assertions.assertEquals("zero", queue.poll());
			Assertions.assertEquals("one", queue.remove());
			Assertions.assertEquals("two", queue.remove());
			Assertions.assertEquals("three", queue.remove());
			Assertions.assertEquals("four", queue.poll());
			Assertions.assertEquals("five", queue.poll());
			Assertions.assertEquals(0L, queue.sizeLong());
			Assertions.assertTrue(queue.isEmpty());
		}
	}

	@Test
	void testPojo() {
		try (XodusQueue<TestPojo> queue = new XodusQueue<>("./test", TestPojo.class)) {
			Assertions.assertEquals(0L, queue.sizeLong());
			queue.add(new TestPojo(0, "zero"));
			Assertions.assertEquals(1L, queue.sizeLong());

			queue.add(new TestPojo(1, "one"));
			queue.add(new TestPojo(2, "two"));
			queue.add(new TestPojo(3, "three"));
			queue.offer(new TestPojo(4, "four"));
			queue.offer(new TestPojo(5, "five"));

			Assertions.assertEquals(6L, queue.sizeLong());
			Assertions.assertEquals(new TestPojo(0, "zero"), queue.element());
			Assertions.assertEquals(new TestPojo(0, "zero"), queue.peek());
			Assertions.assertEquals(new TestPojo(0, "zero"), queue.poll());
			Assertions.assertEquals(new TestPojo(1, "one"), queue.remove());
			Assertions.assertEquals(new TestPojo(2, "two"), queue.remove());
			Assertions.assertEquals(new TestPojo(3, "three"), queue.remove());
			Assertions.assertEquals(new TestPojo(4, "four"), queue.poll());
			Assertions.assertEquals(new TestPojo(5, "five"), queue.poll());
			Assertions.assertEquals(0L, queue.sizeLong());
			Assertions.assertTrue(queue.isEmpty());
		}
	}
}
