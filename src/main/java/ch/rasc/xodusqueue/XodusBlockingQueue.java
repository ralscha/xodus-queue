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

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import ch.rasc.xodusqueue.serializer.XodusQueueSerializer;
import jetbrains.exodus.env.EnvironmentConfig;
import jetbrains.exodus.log.LogConfig;

public class XodusBlockingQueue<T> extends XodusQueue<T> implements BlockingQueue<T> {

	private ReentrantLock reentrantLock;

	/** Condition for waiting takes */
	private Condition notEmpty;

	/** Condition for waiting puts */
	private Condition notFull;

	private long capacity;

	public XodusBlockingQueue(LogConfig logConfig, EnvironmentConfig environmentConfig,
			XodusQueueSerializer<T> serializer, long capacity) {
		super(logConfig, environmentConfig, serializer);
		initLocks(capacity, false);
	}

	public XodusBlockingQueue(String databaseDir, Class<T> entryClass, long capacity) {
		super(databaseDir, entryClass);
		initLocks(capacity, false);
	}

	public XodusBlockingQueue(String databaseDir, XodusQueueSerializer<T> serializer, long capacity) {
		super(databaseDir, serializer);
		initLocks(capacity, false);
	}

	public XodusBlockingQueue(LogConfig logConfig, EnvironmentConfig environmentConfig,
			XodusQueueSerializer<T> serializer, long capacity, boolean fair) {
		super(logConfig, environmentConfig, serializer);
		initLocks(capacity, fair);
	}

	public XodusBlockingQueue(String databaseDir, Class<T> entryClass, long capacity, boolean fair) {
		super(databaseDir, entryClass);
		initLocks(capacity, fair);
	}

	public XodusBlockingQueue(String databaseDir, XodusQueueSerializer<T> serializer, long capacity, boolean fair) {
		super(databaseDir, serializer);
		initLocks(capacity, fair);
	}

	private void initLocks(long capacity, boolean fair) {
		this.capacity = capacity;
		this.reentrantLock = new ReentrantLock(fair);
		this.notEmpty = this.reentrantLock.newCondition();
		this.notFull = this.reentrantLock.newCondition();
	}

	@Override
	public void put(T e) throws InterruptedException {
		Objects.requireNonNull(e);

		final ReentrantLock lock = this.reentrantLock;
		lock.lockInterruptibly();
		try {
			while (super.sizeLong() >= this.capacity) {
				this.notFull.await();
			}
			super.offer(e);
			this.notEmpty.signal();
		}
		finally {
			lock.unlock();
		}
	}

	@Override
	public boolean offer(T e, long timeout, TimeUnit unit) throws InterruptedException {
		Objects.requireNonNull(e);

		long nanos = unit.toNanos(timeout);
		final ReentrantLock lock = this.reentrantLock;
		lock.lockInterruptibly();
		try {
			while (super.sizeLong() >= this.capacity) {
				if (nanos <= 0) {
					return false;
				}
				nanos = this.notFull.awaitNanos(nanos);
			}
			super.offer(e);
			this.notEmpty.signal();
			return true;
		}
		finally {
			lock.unlock();
		}
	}

	@Override
	public boolean offer(T e) {
		Objects.requireNonNull(e);

		final ReentrantLock lock = this.reentrantLock;
		lock.lock();
		try {
			if (super.sizeLong() >= this.capacity) {
				return false;
			}

			boolean result = super.offer(e);
			this.notEmpty.signal();
			return result;
		}
		finally {
			lock.unlock();
		}
	}

	@Override
	public boolean addAll(Collection<? extends T> c) {
		Objects.requireNonNull(c);

		if (c == this) {
			throw new IllegalArgumentException();
		}

		boolean modified = false;
		for (T e : c) {
			if (add(e)) {
				modified = true;
			}
		}
		return modified;
	}

	@Override
	public T poll() {
		final ReentrantLock lock = this.reentrantLock;
		lock.lock();
		try {
			T e = super.poll();
			this.notFull.signal();
			return e;
		}
		finally {
			lock.unlock();
		}
	}

	@Override
	public T take() throws InterruptedException {
		final ReentrantLock lock = this.reentrantLock;
		lock.lockInterruptibly();
		try {
			while (super.sizeLong() == 0) {
				this.notEmpty.await();
			}
			return poll();
		}
		finally {
			lock.unlock();
		}
	}

	@Override
	public T poll(long timeout, TimeUnit unit) throws InterruptedException {
		long nanos = unit.toNanos(timeout);
		final ReentrantLock lock = this.reentrantLock;
		lock.lockInterruptibly();
		try {
			while (super.sizeLong() == 0) {
				if (nanos <= 0) {
					return null;
				}
				nanos = this.notEmpty.awaitNanos(nanos);
			}
			return poll();
		}
		finally {
			lock.unlock();
		}
	}

	@Override
	public int remainingCapacity() {
		final ReentrantLock lock = this.reentrantLock;
		lock.lock();
		try {
			long remaining = this.capacity - super.sizeLong();
			return remaining > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) remaining;
		}
		finally {
			lock.unlock();
		}
	}

	@Override
	public int drainTo(Collection<? super T> c) {
		return drainTo(c, Integer.MAX_VALUE);
	}

	@Override
	public int drainTo(Collection<? super T> c, int maxElements) {
		Objects.requireNonNull(c);

		if (c == this) {
			throw new IllegalArgumentException();
		}

		if (maxElements <= 0) {
			return 0;
		}

		final ReentrantLock lock = this.reentrantLock;
		lock.lock();
		try {
			int n = super.drainTo(c, maxElements);
			for (int i = n; i > 0 && lock.hasWaiters(this.notFull); i--) {
				this.notFull.signal();
			}
			return n;
		}
		finally {
			lock.unlock();
		}
	}

	@Override
	public void clear() {
		final ReentrantLock lock = this.reentrantLock;
		lock.lock();
		try {
			long k = super.sizeLong();
			super.clear();
			for (; k > 0 && lock.hasWaiters(this.notFull); k--) {
				this.notFull.signal();
			}
		}
		finally {
			lock.unlock();
		}
	}

	@Override
	public boolean remove(Object o) {
		final ReentrantLock lock = this.reentrantLock;
		lock.lock();
		try {
			boolean removed = super.remove(o);
			if (removed) {
				this.notFull.signal();
			}
			return removed;
		}
		finally {
			lock.unlock();
		}
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		final ReentrantLock lock = this.reentrantLock;
		lock.lock();
		try {
			long sizeBefore = super.sizeLong();
			boolean removed = super.removeAll(c);
			if (removed) {
				long sizeAfter = super.sizeLong();
				for (long i = sizeBefore - sizeAfter; i > 0 && lock.hasWaiters(this.notFull); i--) {
					this.notFull.signal();
				}
			}
			return removed;
		}
		finally {
			lock.unlock();
		}
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		final ReentrantLock lock = this.reentrantLock;
		lock.lock();
		try {
			long sizeBefore = super.sizeLong();
			boolean changed = super.retainAll(c);
			if (changed) {
				long sizeAfter = super.sizeLong();
				for (long i = sizeBefore - sizeAfter; i > 0 && lock.hasWaiters(this.notFull); i--) {
					this.notFull.signal();
				}
			}
			return changed;
		}
		finally {
			lock.unlock();
		}
	}

}
