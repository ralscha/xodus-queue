package ch.rasc.xodusqueue.example;

import java.util.ArrayList;

import ch.rasc.xodusqueue.XodusBlockingQueue;

/** Run each command in a separate JVM against the same queue directory. */
public class QueueExample {

	public static void main(String[] args) {
		if (args.length < 2) {
			throw new IllegalArgumentException("Usage: QueueExample DIRECTORY depth | add VALUE | drain LIMIT");
		}
		try (var queue = new XodusBlockingQueue<>(args[0], String.class)) {
			switch (args[1]) {
				case "depth" -> System.out.println("queue_depth " + queue.size());
				case "add" -> {
					if (args.length != 3)
						throw new IllegalArgumentException("add requires one value");
					queue.add(args[2]);
				}
				case "drain" -> {
					if (args.length != 3)
						throw new IllegalArgumentException("drain requires a positive limit");
					int limit = Integer.parseInt(args[2]);
					if (limit < 1)
						throw new IllegalArgumentException("limit must be positive");
					var batch = new ArrayList<String>();
					queue.drainTo(batch, limit);
					batch.forEach(System.out::println);
				}
				default -> throw new IllegalArgumentException("Unknown command: " + args[1]);
			}
		}
	}

}
