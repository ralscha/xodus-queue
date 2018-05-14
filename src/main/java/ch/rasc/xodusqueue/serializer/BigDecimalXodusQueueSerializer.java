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
package ch.rasc.xodusqueue.serializer;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ByteIterator;
import jetbrains.exodus.CompoundByteIterable;
import jetbrains.exodus.bindings.IntegerBinding;

public class BigDecimalXodusQueueSerializer implements XodusQueueSerializer<BigDecimal> {

	@Override
	public BigDecimal fromEntry(ByteIterable value) {
		ByteIterator bi = value.iterator();
		int scale = IntegerBinding.readCompressed(bi);
		ArrayByteIterable unscaledValueByteArray = new ArrayByteIterable(bi);
		BigInteger unscaledValue = new BigInteger(
				Arrays.copyOf(unscaledValueByteArray.getBytesUnsafe(),
						unscaledValueByteArray.getLength()));

		return new BigDecimal(unscaledValue, scale);
	}

	@Override
	public ByteIterable toEntry(BigDecimal element) {
		return new CompoundByteIterable(
				new ByteIterable[] { IntegerBinding.intToCompressedEntry(element.scale()),
						new ArrayByteIterable(element.unscaledValue().toByteArray()) });
	}

}
