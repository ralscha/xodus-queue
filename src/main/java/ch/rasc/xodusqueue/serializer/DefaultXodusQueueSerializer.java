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
package ch.rasc.xodusqueue.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.Pool;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;

public class DefaultXodusQueueSerializer<T> implements XodusQueueSerializer<T> {

	private final Pool<Kryo> kryoPool;

	final Class<T> entryClass;

	public DefaultXodusQueueSerializer(final Class<T> entryClass) {
		this.kryoPool = new Pool<>(true, false, 8) {
			@Override
			protected Kryo create() {
				Kryo kryo = new Kryo();
				kryo.register(entryClass);
				return kryo;
			}
		};

		this.entryClass = entryClass;
	}

	@Override
	public T fromEntry(ByteIterable value) {
		ArrayByteIterable abi = new ArrayByteIterable(value);
		try (Input input = new Input(abi.getBytesUnsafe(), 0, abi.getLength())) {
			Kryo kryo = this.kryoPool.obtain();
			try {
				return kryo.readObject(input, this.entryClass);
			}
			finally {
				this.kryoPool.free(kryo);
			}
		}
	}

	@Override
	public ByteIterable toEntry(T element) {
		Kryo kryo = this.kryoPool.obtain();
		try (Output output = new Output(64, -1)) {
			kryo.writeObject(output, element);
			return new ArrayByteIterable(output.toBytes());
		}
		finally {
			this.kryoPool.free(kryo);
		}
	}

}
