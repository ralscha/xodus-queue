[![Build Status](https://api.travis-ci.org/ralscha/xodus-queue.png)](https://travis-ci.org/ralscha/xodus-queue)

This project provides a simple persistent `java.util.Queue` implementation. It is using [Xodus](https://github.com/JetBrains/xodus) as the underlying storage engine. 
For persisting POJOs it relies on [Kryo](https://github.com/EsotericSoftware/kryo).

This is not a high performance queue and it only works within a single JVM. The main motivation was to write a queue that survives a server restart and does 
not introduce a lot of external dependencies to my projects. Because I often use [Xodus](https://github.com/JetBrains/xodus) already in my projects, this library
only adds [Kryo](https://github.com/EsotericSoftware/kryo) as additional dependency. 

Any contributions are welcome, if something is missing or could be implemented better, submit a pull request or an issue.


## Usage

Create an instance of `XodusQueue` and specify the database directory and the class of the entries you want to put into the queue. 
These can be either built in Java types like String, Integer, Long or a more complex POJO. 

You should open the queue in an automatic resource management block, because the underlying Xodus database should be closed 
when you no longer need the queue. 
 
```
try (XodusQueue<String> queue = new XodusQueue<>("./test", String.class)) {

}
```

After the instantiation, you can call any of the methods from the `java.util.Queue<E>` interface.
See the [JavaDoc](https://docs.oracle.com/javase/10/docs/api/java/util/Queue.html) for a list of all available methods.

Currently [`iterator()`](https://docs.oracle.com/javase/10/docs/api/java/util/Collection.html#iterator()) is not implemented.
The underlying storage engine requires that read and write operations have to run inside transactions and I don't know how
to implement that in an iterator. 

```
try (XodusQueue<String> queue = new XodusQueue<>("./queue", String.class)) {
  queue.add("one");

  String head = queue.poll(); // "one"
}
```


## Maven
The library is hosted on the Central Maven Repository
```
	<dependency>
		<groupId>ch.rasc</groupId>
		<artifactId>xodus-queue</artifactId>
		<version>1.0.0</version>
	</dependency>
```


## Changelog

### 1.0.0 - May ??, 2018
  * Initial release


## License
Code released under [the Apache license](http://www.apache.org/licenses/).