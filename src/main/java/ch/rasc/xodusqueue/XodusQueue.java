/**
 * Copyright 2018-2022 the original author or authors.
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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import ch.rasc.xodusqueue.serializer.BigDecimalXodusQueueSerializer;
import ch.rasc.xodusqueue.serializer.BigIntegerXodusQueueSerializer;
import ch.rasc.xodusqueue.serializer.BooleanXodusQueueSerializer;
import ch.rasc.xodusqueue.serializer.ByteXodusQueueSerializer;
import ch.rasc.xodusqueue.serializer.DefaultXodusQueueSerializer;
import ch.rasc.xodusqueue.serializer.DoubleXodusQueueSerializer;
import ch.rasc.xodusqueue.serializer.FloatXodusQueueSerializer;
import ch.rasc.xodusqueue.serializer.IntegerXodusQueueSerializer;
import ch.rasc.xodusqueue.serializer.LongXodusQueueSerializer;
import ch.rasc.xodusqueue.serializer.ShortXodusQueueSerializer;
import ch.rasc.xodusqueue.serializer.StringXodusQueueSerializer;
import ch.rasc.xodusqueue.serializer.XodusQueueSerializer;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.bindings.LongBinding;
import jetbrains.exodus.env.Cursor;
import jetbrains.exodus.env.Environment;
import jetbrains.exodus.env.EnvironmentConfig;
import jetbrains.exodus.env.Environments;
import jetbrains.exodus.env.Store;
import jetbrains.exodus.env.StoreConfig;
import jetbrains.exodus.env.Transaction;
import jetbrains.exodus.env.TransactionalComputable;
import jetbrains.exodus.log.LogConfig;

public class XodusQueue<T> extends AbstractQueue<T> implements AutoCloseable {

	private static final String STORE_NAME = "queue";

	private final Environment env;

	private final XodusQueueSerializer<T> serializer;

	private final AtomicLong key = new AtomicLong(0L);

	@SuppressWarnings("unchecked")
	public XodusQueue(final String databaseDir, final Class<T> entryClass) {
		this.env = Environments.newInstance(databaseDir);

		if (entryClass == String.class) {
			this.serializer = (XodusQueueSerializer<T>) new StringXodusQueueSerializer();
		}
		else if (entryClass == Integer.class) {
			this.serializer = (XodusQueueSerializer<T>) new IntegerXodusQueueSerializer();
		}
		else if (entryClass == Long.class) {
			this.serializer = (XodusQueueSerializer<T>) new LongXodusQueueSerializer();
		}
		else if (entryClass == Boolean.class) {
			this.serializer = (XodusQueueSerializer<T>) new BooleanXodusQueueSerializer();
		}
		else if (entryClass == Byte.class) {
			this.serializer = (XodusQueueSerializer<T>) new ByteXodusQueueSerializer();
		}
		else if (entryClass == Double.class) {
			this.serializer = (XodusQueueSerializer<T>) new DoubleXodusQueueSerializer();
		}
		else if (entryClass == Float.class) {
			this.serializer = (XodusQueueSerializer<T>) new FloatXodusQueueSerializer();
		}
		else if (entryClass == Short.class) {
			this.serializer = (XodusQueueSerializer<T>) new ShortXodusQueueSerializer();
		}
		else if (entryClass == BigInteger.class) {
			this.serializer = (XodusQueueSerializer<T>) new BigIntegerXodusQueueSerializer();
		}
		else if (entryClass == BigDecimal.class) {
			this.serializer = (XodusQueueSerializer<T>) new BigDecimalXodusQueueSerializer();
		}
		else {
			this.serializer = new DefaultXodusQueueSerializer<>(entryClass);
		}

		this.key.set(this.sizeLong());
	}

	public XodusQueue(final String databaseDir,
			final XodusQueueSerializer<T> serializer) {
		this.env = Environments.newInstance(databaseDir);
		this.serializer = serializer;

		this.key.set(this.sizeLong());
	}

	public XodusQueue(final LogConfig logConfig,
			final EnvironmentConfig environmentConfig,
			final XodusQueueSerializer<T> serializer) {
		this.env = Environments.newInstance(logConfig, environmentConfig);
		this.serializer = serializer;

		this.key.set(this.sizeLong());
	}

	@Override
	public boolean offer(T e) {
		Objects.requireNonNull(e);

		this.env.executeInExclusiveTransaction(txn -> {
			Store store = this.env.openStore(STORE_NAME, StoreConfig.WITHOUT_DUPLICATES,
					txn);

			if (store.count(txn) == 0L) {
				this.key.set(0L);
			}

			store.putRight(txn, LongBinding.longToEntry(this.key.incrementAndGet()),
					this.serializer.toEntry(e));
		});

		return true;
	}

	@Override
	public boolean addAll(Collection<? extends T> c) {
		Objects.requireNonNull(c);

		if (c == this) {
			throw new IllegalArgumentException();
		}

		return this.env.computeInExclusiveTransaction(txn -> {
			Store store = this.env.openStore(STORE_NAME, StoreConfig.WITHOUT_DUPLICATES,
					txn);

			if (store.count(txn) == 0L) {
				this.key.set(0L);
			}

			boolean modified = false;
			for (T e : c) {
				store.putRight(txn, LongBinding.longToEntry(this.key.incrementAndGet()),
						this.serializer.toEntry(e));
				modified = true;
			}

			return modified;
		});
	}

	@Override
	public T poll() {
		return this.env.computeInExclusiveTransaction(pollComputable(true));
	}

	@Override
	public T peek() {
		return this.env.computeInReadonlyTransaction(pollComputable(false));
	}

	private TransactionalComputable<T> pollComputable(final boolean remove) {
		return txn -> {
			Store store = this.env.openStore(STORE_NAME, StoreConfig.WITHOUT_DUPLICATES,
					txn, false);
			if (store != null) {
				try (Cursor cursor = store.openCursor(txn)) {
					if (cursor.getNext()) {
						ByteIterable value = cursor.getValue();
						if (remove) {
							cursor.deleteCurrent();
						}

						return this.serializer.fromEntry(value);
					}
				}
			}
			return null;
		};
	}

	@Override
	public int size() {
		return (int) sizeLong();
	}

	public long sizeLong() {
		return this.env.computeInReadonlyTransaction(txn -> {
			Store store = this.env.openStore(STORE_NAME, StoreConfig.WITHOUT_DUPLICATES,
					txn, false);
			if (store != null) {
				return store.count(txn);
			}
			return 0L;
		});
	}

	@Override
	public void close() {
		if (this.env != null) {
			this.env.close();
		}
	}

	@Override
	public boolean isEmpty() {
		return sizeLong() == 0;
	}

	@Override
	public boolean contains(Object o) {
		return this.env.computeInReadonlyTransaction(txn -> {
			Store store = this.env.openStore(STORE_NAME, StoreConfig.WITHOUT_DUPLICATES,
					txn, false);
			if (store != null) {
				return containsInternal(o, txn, store);
			}
			return false;
		});
	}

	private boolean containsInternal(Object o, Transaction txn, Store store) {
		try (Cursor cursor = store.openCursor(txn)) {
			while (cursor.getNext()) {
				T e = this.serializer.fromEntry(cursor.getValue());
				if (e.equals(o)) {
					return true;
				}
			}
		}
		return false;
	}

	@Override
	public Iterator<T> iterator() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object[] toArray() {
		return this.env.computeInReadonlyTransaction(txn -> {
			Store store = this.env.openStore(STORE_NAME, StoreConfig.WITHOUT_DUPLICATES,
					txn, false);
			if (store != null) {
				Object[] r = new Object[(int) store.count(txn)];
				int ix = 0;
				try (Cursor cursor = store.openCursor(txn)) {
					while (cursor.getNext()) {
						ByteIterable value = cursor.getValue();
						T e = this.serializer.fromEntry(value);
						r[ix++] = e;
					}
				}
				return r;
			}
			return new Object[0];
		});
	}

	@SuppressWarnings({ "unchecked", "hiding" })
	@Override
	public <T> T[] toArray(T[] a) {
		return this.env.computeInReadonlyTransaction(txn -> {
			Store store = this.env.openStore(STORE_NAME, StoreConfig.WITHOUT_DUPLICATES,
					txn, false);
			if (store != null) {
				int size = (int) store.count(txn);
				T[] r = a.length >= size ? a
						: (T[]) java.lang.reflect.Array
								.newInstance(a.getClass().getComponentType(), size);
				int ix = 0;
				try (Cursor cursor = store.openCursor(txn)) {
					while (cursor.getNext()) {
						ByteIterable value = cursor.getValue();
						r[ix++] = (T) this.serializer.fromEntry(value);
					}
				}
				return r;
			}
			return a;
		});
	}

	@Override
	public boolean remove(Object o) {
		return this.env.computeInExclusiveTransaction(txn -> {
			Store store = this.env.openStore(STORE_NAME, StoreConfig.WITHOUT_DUPLICATES,
					txn, false);

			if (store != null) {
				try (Cursor cursor = store.openCursor(txn)) {
					while (cursor.getNext()) {
						T e = this.serializer.fromEntry(cursor.getValue());
						if (o.equals(e)) {
							cursor.deleteCurrent();
							return true;
						}
					}
				}
			}
			return false;
		});
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		return this.env.computeInReadonlyTransaction(txn -> {
			Store store = this.env.openStore(STORE_NAME, StoreConfig.WITHOUT_DUPLICATES,
					txn, false);
			if (store != null) {
				for (Object e : c) {
					if (!containsInternal(e, txn, store)) {
						return false;
					}
				}
				return true;
			}
			return false;
		});
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		Objects.requireNonNull(c);

		return this.env.computeInExclusiveTransaction(txn -> {
			Store store = this.env.openStore(STORE_NAME, StoreConfig.WITHOUT_DUPLICATES,
					txn, false);
			boolean modified = false;
			if (store != null) {
				try (Cursor cursor = store.openCursor(txn)) {
					while (cursor.getNext()) {
						T e = this.serializer.fromEntry(cursor.getValue());
						if (c.contains(e)) {
							cursor.deleteCurrent();
							modified = true;
						}
					}
				}
			}
			return modified;
		});
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		Objects.requireNonNull(c);

		return this.env.computeInExclusiveTransaction(txn -> {
			Store store = this.env.openStore(STORE_NAME, StoreConfig.WITHOUT_DUPLICATES,
					txn, false);
			boolean modified = false;
			if (store != null) {
				try (Cursor cursor = store.openCursor(txn)) {
					while (cursor.getNext()) {
						T e = this.serializer.fromEntry(cursor.getValue());
						if (!c.contains(e)) {
							cursor.deleteCurrent();
							modified = true;
						}
					}
				}
			}
			return modified;
		});
	}

	@Override
	public void clear() {
		this.env.executeInExclusiveTransaction(txn -> {
			this.env.truncateStore(STORE_NAME, txn);
		});
	}

	protected int drainTo(Collection<? super T> c, int maxElements) {
		Objects.requireNonNull(c);

		if (c == this) {
			throw new IllegalArgumentException();
		}

		if (maxElements <= 0) {
			return 0;
		}

		return this.env.computeInExclusiveTransaction(txn -> {
			Store store = this.env.openStore(STORE_NAME, StoreConfig.WITHOUT_DUPLICATES,
					txn, false);
			if (store != null) {
				int currentCounter = 0;
				try (Cursor cursor = store.openCursor(txn)) {
					while (cursor.getNext() && currentCounter < maxElements) {
						T e = this.serializer.fromEntry(cursor.getValue());
						c.add(e);
						cursor.deleteCurrent();
						currentCounter++;
					}
				}
				return currentCounter;
			}
			return 0;
		});

	}

}
