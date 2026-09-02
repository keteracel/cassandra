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

/**
 * Executes co-located with the primary natural replica for a key (see {@code EntryOwnership}).
 * <p>
 * A processor reads the current state of its entry from {@link EntryProcessorContext#currentRow()} and writes
 * only the columns it changes via {@link EntryProcessorContext#delta()}. That delta is what gets replicated to
 * the key's other natural replicas through Cassandra's normal write path — there is no separate backup-processor
 * type; replicas never re-run processor logic.
 *
 * @param <R> the type of the result returned to the invoking client.
 */
public interface EntryProcessor<R>
{
    R process(EntryProcessorContext ctx) throws Exception;
}
