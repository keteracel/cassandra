/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.compute;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;

/**
 * Resolves an {@link EntryProcessor} from the class reference carried in an {@link EntryProcessorRequest}.
 * <p>
 * Per the product spec's decision, processors are deployed ahead of time to every node's classpath and referenced
 * by name — there is no dynamic loading here (contrast with Cassandra's own {@code triggers} package, which loads
 * classes at runtime via a dedicated classloader; deliberately not reused, see the technical spec's Out of Scope).
 * A processor class needs either a constructor taking a single {@link ByteBuffer} (used when the request carries
 * non-empty {@code processorInitArgs}) or a no-arg constructor.
 */
final class ProcessorRegistry
{
    private ProcessorRegistry()
    {
    }

    @SuppressWarnings("unchecked")
    static EntryProcessor<ByteBuffer> instantiate(String className, ByteBuffer initArgs) throws ReflectiveOperationException
    {
        Class<?> clazz = Class.forName(className);

        if (initArgs != null && initArgs.hasRemaining())
        {
            try
            {
                Constructor<?> ctor = clazz.getConstructor(ByteBuffer.class);
                return (EntryProcessor<ByteBuffer>) ctor.newInstance(initArgs.duplicate());
            }
            catch (NoSuchMethodException e)
            {
                // fall through to the no-arg constructor
            }
        }

        return (EntryProcessor<ByteBuffer>) clazz.getDeclaredConstructor().newInstance();
    }
}
