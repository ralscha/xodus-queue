![Test Status](https://github.com/ralscha/xodus-queue/workflows/test/badge.svg)

This project provides a persistent `java.util.Queue` and `java.util.concurrent.BlockingQueue` implementation. It is using [Xodus](https://github.com/JetBrains/xodus) as the underlying storage engine. 
For persisting POJOs, it relies on [Kryo](https://github.com/EsotericSoftware/kryo).

xodus-queue is not a high-performance queue, and it only works within a single JVM. The primary motivation was to write a queue that survives a server restart and does not introduce a lot of external dependencies to my projects. Because I often use [Xodus](https://github.com/JetBrains/xodus) already in my projects, this library
only adds [Kryo](https://github.com/EsotericSoftware/kryo) as an additional dependency. 

Any contributions are welcome if something is missing or could be implemented better, submit a pull request, or create an issue.


## Usage

Create an instance of `XodusQueue` or `XodusBlockingQueue` and specify the database directory and the class of the entries you want to put into the queue. 
These can be either built-in Java types like String, Integer, Long, or a more complex POJO. 

It is recommended to open the queue in an automatic resource management block because the underlying Xodus database should be closed when you no longer access the queue. 
 
```
try (XodusQueue<String> queue = new XodusQueue<>("./test", String.class)) {

}
```

After the instantiation, you can call any of the methods from the `java.util.Queue<E>` and `java.util.concurrent.BlockingQueue<E>` interface.
See the JavaDoc ([Queue](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/Queue.html), [BlockingQueue](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/concurrent/BlockingQueue.html)) for a list of all available methods.

The `iterator()` method returns a snapshot of the queue contents at the time the iterator is created.

```
try (XodusQueue<String> queue = new XodusQueue<>("./queue", String.class)) {
  queue.add("one");

  String head = queue.poll(); // "one"
}
```

The blocking queue supports a capacity limit. The following example limits the number of elements in the queue to 3. 
`put` blocks the current thread when the queue is full and `take` blocks when the queue is empty.
The capacity must be greater than zero.
```
try (XodusBlockingQueue<String> queue = new XodusBlockingQueue<>("./blocking_queue", String.class, 3)) {
  queue.put("one");
  queue.put("two");

  String head = queue.take(); // "one"
}
```


## Maven
The library is hosted on the Central Maven Repository
```
  <dependency>
    <groupId>ch.rasc</groupId>
    <artifactId>xodus-queue</artifactId>
    <version>2.0.0</version>
  </dependency>
```


## Changelog

### 2.0.0 - Sep 6, 2025
  * Minimum Java 17
  * Fix concurrency issues
  * Upgrade Kryo library

### 1.0.1 - May 19, 2018
  * Fix key management in XodusQueue
  * Add `java.util.concurrent.BlockingQueue` implementation: XodusBlockingQueue

### 1.0.0 - May 15, 2018
  * Initial release


## License
Code released under [the Apache license](http://www.apache.org/licenses/).
