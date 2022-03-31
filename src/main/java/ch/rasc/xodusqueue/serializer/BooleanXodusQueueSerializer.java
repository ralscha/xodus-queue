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
package ch.rasc.xodusqueue.serializer;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.bindings.BooleanBinding;

public class BooleanXodusQueueSerializer implements XodusQueueSerializer<Boolean> {

	@Override
	public Boolean fromEntry(ByteIterable value) {
		return BooleanBinding.entryToBoolean(value);
	}

	@Override
	public ByteIterable toEntry(Boolean element) {
		return BooleanBinding.booleanToEntry(element);
	}

}
