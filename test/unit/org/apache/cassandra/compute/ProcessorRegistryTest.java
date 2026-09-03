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

import java.nio.ByteBuffer;

import org.junit.Test;

import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * {@link ProcessorRegistry} is package-private (hence this test living in {@code org.apache.cassandra.compute}
 * rather than under a {@code test.compute} package of its own). Before this test, two of its three branches had
 * never executed anywhere:
 * <ul>
 *     <li>An unknown/misspelled processor class name - the {@link ClassNotFoundException} path that
 *     {@link EntryProcessorRequestHandler#execute} turns into {@code PROCESSOR_FAILURE}.</li>
 *     <li>The {@code (ByteBuffer)}-arg constructor path, used when a request carries non-empty
 *     {@code processorInitArgs}. In practice this is currently unreachable from any real caller -
 *     {@code EntryProcessorCallStatement} always sends {@link ByteBufferUtil#EMPTY_BYTE_BUFFER} (there's no CQL
 *     syntax to supply init args), and the existing dtest's {@code invokeIncrement} helper does the same - so this
 *     was genuinely dead-in-tests code before now.</li>
 * </ul>
 */
public class ProcessorRegistryTest
{
    @Test
    public void unknownClassNameThrowsReflectiveOperationException()
    {
        try
        {
            ProcessorRegistry.instantiate("com.example.DefinitelyDoesNotExist", ByteBufferUtil.EMPTY_BYTE_BUFFER);
            fail("expected instantiate() to throw for an unknown class name");
        }
        catch (ReflectiveOperationException expected)
        {
            assertTrue("expected a ClassNotFoundException specifically, got " + expected.getClass(),
                       expected instanceof ClassNotFoundException);
        }
    }

    @Test
    public void byteBufferConstructorIsSelectedWhenInitArgsAreNonEmpty() throws Exception
    {
        ByteBuffer initArgs = ByteBufferUtil.bytes("hello-init-arg");

        EntryProcessor<ByteBuffer> processor =
            ProcessorRegistry.instantiate(BothConstructorsProcessor.class.getName(), initArgs);

        assertTrue(processor instanceof BothConstructorsProcessor);
        BothConstructorsProcessor typed = (BothConstructorsProcessor) processor;
        assertEquals("the (ByteBuffer) constructor should have been used, not the no-arg one",
                     initArgs, typed.receivedInitArg);
    }

    @Test
    public void noArgConstructorIsSelectedWhenInitArgsAreEmpty() throws Exception
    {
        EntryProcessor<ByteBuffer> processor =
            ProcessorRegistry.instantiate(BothConstructorsProcessor.class.getName(), ByteBufferUtil.EMPTY_BYTE_BUFFER);

        assertTrue(processor instanceof BothConstructorsProcessor);
        assertNull("the no-arg constructor should have been used - empty init args, per the initArgs.hasRemaining() check",
                   ((BothConstructorsProcessor) processor).receivedInitArg);
    }

    @Test
    public void nullInitArgsAlsoSelectsNoArgConstructor() throws Exception
    {
        EntryProcessor<ByteBuffer> processor = ProcessorRegistry.instantiate(BothConstructorsProcessor.class.getName(), null);

        assertNull(((BothConstructorsProcessor) processor).receivedInitArg);
    }

    @Test
    public void fallsBackToNoArgConstructorWhenNoByteBufferConstructorExists() throws Exception
    {
        // Non-empty init args, but the class only has a no-arg constructor - ProcessorRegistry must catch the
        // NoSuchMethodException from getConstructor(ByteBuffer.class) and fall through, not propagate it.
        EntryProcessor<ByteBuffer> processor =
            ProcessorRegistry.instantiate(NoArgOnlyProcessor.class.getName(), ByteBufferUtil.bytes("ignored"));

        assertTrue(processor instanceof NoArgOnlyProcessor);
    }

    public static final class BothConstructorsProcessor implements EntryProcessor<ByteBuffer>
    {
        final ByteBuffer receivedInitArg;

        public BothConstructorsProcessor()
        {
            this.receivedInitArg = null;
        }

        public BothConstructorsProcessor(ByteBuffer initArg)
        {
            this.receivedInitArg = initArg;
        }

        @Override
        public ByteBuffer process(EntryProcessorContext ctx)
        {
            throw new UnsupportedOperationException("not exercised by this test");
        }
    }

    public static final class NoArgOnlyProcessor implements EntryProcessor<ByteBuffer>
    {
        public NoArgOnlyProcessor()
        {
        }

        @Override
        public ByteBuffer process(EntryProcessorContext ctx)
        {
            throw new UnsupportedOperationException("not exercised by this test");
        }
    }
}
