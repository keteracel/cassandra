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

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.Row;

/**
 * Passed to an {@link EntryProcessor} for a single invocation. {@link #currentRow()} is read locally — execution
 * always happens at the key's primary natural replica, which already holds the authoritative local copy, so no
 * read consistency level is involved. {@link #delta()} is the only way to produce a change: it exposes Cassandra's
 * own {@link Row.SimpleBuilder}, so a processor can only add or delete individual columns, never replace the row
 * wholesale — this is what keeps the resulting mutation a genuine partial update.
 */
public interface EntryProcessorContext
{
    DecoratedKey key();

    Row currentRow();

    Row.SimpleBuilder delta();
}
