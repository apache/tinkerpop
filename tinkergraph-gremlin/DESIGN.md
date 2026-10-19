<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# TinkerStorageGraph Storage Design

How `TinkerStorageGraph` keeps its data on disk, written for contributors and for anyone who needs to reason about
what is on disk after a failure. What a user can rely on, and how to configure and recover a store, is in the
Persistence section of the TinkerGraph reference documentation. This document covers the machinery behind those
guarantees: the files, their formats, the order of writes, and the checks made when a store is opened.

## Model

The graph in memory is authoritative. Storage is a write-ahead mirror of it: each commit is written to disk before it
is applied in memory, and opening a store rebuilds the in-memory graph from what is on disk. A persisted graph must
therefore fit in memory.

A storage engine implements `TinkerStorage`: `open`, `replay`, `persist`, `flush`, `compact`, `maybeCompact`, `clear`,
`isReadOnly`, `isFailed` and `close`. `AbstractLogStorage` implements everything that does not depend on how an
element is encoded: the files, framing, checksums, replay, sync modes, compaction, recovery and failure handling.
`GraphBinaryStorage`, the `graphbinary` engine, supplies only the codec (`encodeCommit`, `decodeFrame`,
`writeSnapshot`, `beginReplay`).

## Files

A storage directory holds:

| File | Contents | Written |
|---|---|---|
| `LOCK` | a random 16-byte token written by the graph that holds it; held with a file lock while a graph has the store open | at open |
| `VERSION` | magic `TGSB`, format version byte (currently `1`), store id (16-byte UUID), CRC32 of those bytes | when the store is created |
| `settings.properties` | the data-shaping settings the store was created with, as Java properties | when the store is created |
| `snapshot.gbin` | file header, then a dictionary frame and one frame per element | by compaction |
| `log.gbin` | file header, then one frame per commit since the last compaction | appended by every commit |
| `INDEXES` | one indexed key per line, `V<TAB>key` or `E<TAB>key`, with `\`, tab and newlines escaped | when an index is created or dropped |

Every file other than the log is replaced whole: written to `<name>.tmp`, forced to the device, renamed over the
target with an atomic move, and followed by a sync of the directory. A crash therefore leaves either the old file or
the new one. A `.tmp` file left behind by an interrupted write holds nothing the store needs, and a normal open removes
any stale `snapshot.gbin.tmp`, `log.gbin.tmp` or `VERSION.tmp`. A directory that cannot be synced on some network and
FUSE mounts is logged as a warning rather than failing the write, since only the rename's durability is in doubt.

The snapshot and the log begin with the same 32-byte file header: the magic, an 8-byte generation, the store id and a
CRC32 of those bytes. The store id ties the files to one store, so a snapshot or log copied in from another store is
detected. The generation counts compactions and ties a log to the snapshot it follows. The checksum keeps a damaged
header from reading as a believable one, since a flipped bit in the generation would otherwise make an intact log look
as if it belonged to another snapshot. A snapshot whose header is damaged fails the open, recovery included, because
nothing in it can be trusted to belong to the store. A damaged log header fails a normal open, and recovery reads the
snapshot alone. When the snapshot and log agree on their store but `VERSION` names another, the error names `VERSION`
as the file that does not belong. `VERSION` carries a checksum of its own for the same reason, so a damaged marker is
reported as damaged rather than as one from another store.

## Frames

A snapshot frame is a 4-byte payload length, a 4-byte CRC32 of the payload, and the payload. A log frame is a 4-byte
payload length, a 4-byte CRC32 of that length, a 4-byte sequence number, a 4-byte CRC32 of the sequence number and
payload together, and the payload, so a log frame header is 16 bytes. Checksumming the length lets replay tell a frame
whose length was damaged apart from an append that was cut short: without it, a flipped bit that made a length claim
more bytes than remain would read as an interrupted append and silently drop every later commit. The sequence number
is the frame's position in its log, counting from 0 after every new log is installed. Replay requires it to match, so
an intact record that was repeated, reordered or copied in from another log fails a normal open, and a recovery open
stops at it, rather than replaying a transaction out of place.

A payload is a varint entry count followed by that many entries, each an op byte and its body:

| Op | Body |
|---|---|
| `1` put vertex | vertex record |
| `2` delete vertex | id |
| `3` put edge | edge record |
| `4` delete edge | id |
| `5` dictionary append | varint id, varint length, UTF-8 bytes |

A vertex record is the id, a varint label count and the label refs, a byte saying whether vertex-property ids follow
each value, and then for each property key its ref, a varint value count, and per value the value, its id when the
flag is set, and a varint count of meta-properties, each a key ref and a value. An edge record is the id, the label
ref, the out-vertex id, the in-vertex id, and a varint property count, each a key ref and a value.

Ids and values are GraphBinary scalars: a one-byte type code and the value without its value flag, with
`UNSPECIFIED_NULL` for a null. Labels and property keys are dictionary refs instead of strings. Replay applies records
in order with last write wins per element id, the snapshot first and then the log.

### Dictionary

Labels, property keys and meta-property keys share one dictionary of dense ids assigned in order of first appearance.
A commit that introduces a string emits its dictionary append inside the same frame, ahead of the entries that use it,
so a torn frame loses an entry and its users together. Numbering never changes. Compaction writes the whole
dictionary as the snapshot's first frame and keeps the existing ids. No log is replayed against a newer snapshot, since
the old log left by an interrupted compaction is set aside unread, so the numbering would survive a renumbering too; it
is kept because doing so costs nothing. If encoding a commit fails,
for any exception or error, the entries it added are removed again so that a later commit cannot refer to an id that
never reached disk.

### Bounded decoding

A frame's length is checked against the file before its payload is allocated. Inside a payload every length a value
declares, for strings, binary values, `BigInteger`, lists, sets and maps, is checked against the bytes left in the
record before it is allocated, at the top level, inside collections and in the parts a serializer reads with their
type already known, such as the label list of a vertex or edge value, and a bulked list, which the engine never
writes, is rejected. A value cut off by the end of its record, or one the serializers cannot read, is reported as a
corrupt frame naming the file rather than surfacing as a runtime exception or exhausting memory.

## Commit

A transaction that changed nothing does not touch storage. Otherwise, under the graph's `storageCommitLock`:

1. `persist` encodes the changeset into one frame and appends it to the log, opening the log if needed.
2. `flush` flushes the stream and, under `sync=commit`, forces it to the device, then checks that the store is still in
   place.
3. The indexes are committed, then the elements are published in memory.
4. `maybeCompact` compacts if the log has grown past the threshold.

The in-memory publish is held inside the lock because compaction snapshots the graph and then discards the log. A
compaction landing between a persisted and a published commit would snapshot without that commit and delete the
record that held it. Holding the publish under the lock closes that window and makes commits become visible in the
order they were recorded. Indexes are committed only after the write succeeds, because rollback can undo only a
transaction's own index changes, not a shared index already committed.

A commit that cannot be encoded fails on its own and the engine carries on, since nothing was written. A failure to
write or flush the log stops the engine (fail-stop): what reached disk is unknown, so every later commit is refused
until the graph is reopened and replays what is actually there. A failed automatic compaction does not fail the commit
that triggered it. The next attempt waits for another threshold's worth of log. An `Error` from it, such as running out
of memory while writing the snapshot, is not swallowed: `commit()` throws it as it is, and the transaction is committed
and the engine still accepts commits. A failure of any kind after the new snapshot is renamed into place, while the
empty log is installed, stops the engine, since the old log can then only stay as it is.

Each frame is held in one buffer, so a single commit, or a single element in a snapshot, is limited to
`Integer.MAX_VALUE - 8` bytes. A larger one fails during encoding, before anything is written.

## Compaction

Compaction closes the log, writes `snapshot.gbin.tmp` with the next generation, the dictionary frame and one frame per
committed element, forces and renames it over the snapshot and syncs the directory. It then installs a header-only log
of the same new generation through `log.gbin.tmp`, and records the new log's identity. The log is never deleted, so a
store with data always has a log, and a missing log is always damage.

A crash after the snapshot rename but before the new log is installed leaves a snapshot one generation ahead of its
log. That log's commits are already in the snapshot, so open completes the compaction by installing the empty log.

Compaction reads committed state only (`getUnmodified`), never a thread's open transaction. With nothing logged since
the last compaction it does nothing, since the snapshot already holds every commit, so a `close()` after a session that
only read leaves the store untouched.

## Opening a Store

1. The directory is locked: exclusively, or, for a recover open of a directory the process cannot write, with a shared
   lock on an existing `LOCK` file or with no lock at all.
2. The configured data-shaping settings are validated as the graph will read them, then reconciled with
   `settings.properties`: a new store records them, an existing store fills unset ones from the record and refuses
   any that differ, and a record missing a fixed key, or holding a value no store is created with (not a built-in
   name, or for the id managers and the engine, not a class name), is treated as damaged. A first open that fails
   removes the settings and `VERSION` it wrote, so the corrected open is again a first open.
3. `VERSION` is created for a new store, or read and checked. Data without a `VERSION` fails a normal open.
4. The snapshot and log headers are compared:

   | Snapshot | Log | Result |
   |---|---|---|
   | absent | absent | a new store gets an empty log of generation 0; a store created by an earlier open has lost its log and fails |
   | present | absent | the log was removed and fails |
   | absent | generation 0 | the log alone holds the data |
   | absent | generation above 0 | the snapshot was removed and fails |
   | generation G | generation G | normal |
   | generation G + 1 | generation G | compaction crash window, completed by installing an empty log |
   | other | other | the files do not belong together and fail |

   A store id that differs from `VERSION` fails the open as well.
5. The snapshot and then the log are replayed into the graph, and the recorded indexes are rebuilt over the result.

### Torn Log Tails

A crash during an append leaves a log that ends partway through a frame. That commit was never acknowledged, so
replay stops at the end of the last complete frame and the log is cut back to it, forced to the device, before
anything new is appended. Otherwise new commits would land after the torn bytes and the next replay would stop there
and drop them. Replay treats these endings as torn:

- fewer bytes than a frame header remain;
- a header whose length passes its checksum but claims more bytes than remain;
- every byte after the last complete frame is zero, which some filesystems leave when a crash extends a file before its
  data is written (an all-zero header can never be valid, since the CRC32 of length 0 is `0x2144DF1C`);
- the last frame fails its payload checksum, ends exactly at the end of the log, and every byte from the log's last
  512-byte boundary to its end is zero and part of that frame's payload, the state a crash leaves when the header
  reached the device but the end of the payload did not. Devices write whole sectors, so data that never reached one
  reads back as zeros from a sector boundary on.

Anything else that fails a checksum is damage and fails a normal open, including a damaged final frame that merely ends
in a zero byte, as most vertex and edge records do. The cost is that a payload torn within its last sector, with the
zeros starting past the boundary or the whole payload shorter than a sector, also fails the open and needs a recover
open. Only the final frame is ever cut by the last rule, and under `sync=commit` an acknowledged commit is lost that
way only if the device zeroed the last sector of a record it had already been forced to hold. A log damaged so that it ends exactly on a frame boundary, such as one
cut back to its header, reads as a log with fewer commits, and the loss is not detected. The sequence numbers do not
change that, since what remains is still numbered from 0.

## Recovery Mode

An open with `gremlin.tinkergraph.storage.recover` writes nothing: no `VERSION`, no settings, no truncation, no
compaction, and no commits. Each log frame is applied atomically through a journaled map, and replay stops at the first
frame that cannot be read, keeping every transaction before it. An unreadable snapshot frame is skipped, except the
first, which holds the dictionary every other record depends on, and edges that lose an endpoint as a result are
dropped. A log that is missing, or belongs to another snapshot or store, is ignored and the snapshot read alone. A
`VERSION` that is missing or damaged is tolerated: the store is then read as the current format, with the store id of
its snapshot, so a log from another store is still set aside. What was left out is reported in one warning.

A recovered graph still accepts in-memory changes and refuses them at commit, and `createIndex`, `dropIndex` and
`clear()` work in memory without being recorded. Reads, and commits of transactions that only read, work as usual.

## Removed or Replaced Stores

The log's file identity (`BasicFileAttributes.fileKey()`) is recorded when a store is opened and again whenever
compaction installs a new log. It is checked before the log is opened for append, after each flush, and at
compaction, along with the presence of the directory and the log. A change means the store was removed or replaced
while the graph was open, possibly by another graph that has since created a store at the same path, and the engine
fail-stops with a `StoreRemovedException`. From then on nothing is written to the directory, not even by `close()`,
and the committed data stays readable in memory for export. On a platform without file keys only the presence checks
apply.

The `LOCK` file's identity is recorded at open and checked with the log's, and also before each commit is appended, so
a graph whose `LOCK` was deleted or replaced, which lets a second graph lock a new file at that path and open the
store, fail-stops before it writes anything more. Acquiring the lock writes a random token through the locked channel
and reads it back through the path. A graph whose open fails removes the `LOCK` it created, so without that check a
graph could hold a lock on a file that has just been unlinked and keep no one out. A recovery open skips the token,
since it leaves every file as it found it and writes nothing to the store. The handle the token is read back through
stays open as long as the lock is held, because closing any handle on a file releases every POSIX lock the process
holds on it while the JVM goes on reporting the lock held. The lock covers a single byte past the token rather than
the whole file, because on Windows a lock keeps every other handle from reading the bytes it covers. For the same
POSIX reason a second open of a directory a graph in this JVM already holds is refused before any handle on its `LOCK`
is opened, by a process-wide record of the directories held: a refused open that opened a handle would have to close
it, releasing the holder's lock.

A commit that finds the store moved fails and is rolled back in memory, although its frame may already be durable at
the store's new location, because the check follows the forced write.

## Fail-Stop

`isFailed()` reports that the engine has stopped after a write failure or a removed store. A fail-stopped graph keeps
its data readable for export. It refuses commits that change anything, explicit `compact()` and `clear()`, and it
changes indexes in memory only, without writing `INDEXES`.

The graph also fail-stops the engine through `failStop()` when a commit's changes were persisted and flushed but
applying them in memory (the index or element commit) then failed. The durable frame is the commit point, as a commit
record in a database's log is, so the transaction is not reported as rolled back: `doCommit` throws a
`TransactionException` saying that it was recorded and will be present after a reopen. Memory keeps the state from
before the transaction, which no longer matches the log, so nothing more may be written from it. A fail-stopped engine
neither commits nor compacts, `close()` included, and the reopen replays the frame.

## `clear()`

`clear()` keeps its in-memory meaning: the whole graph is emptied at once, outside any transaction. Under the commit
lock it rolls back the calling thread's open transaction, marks every element container removed so that a
transaction on another thread that still holds one fails to commit with a conflict, clears memory, and then calls
`TinkerStorage.clear`, which for `AbstractLogStorage` compacts the now-empty graph and rewrites `INDEXES`. That
compaction runs even with nothing logged since the last one, since the snapshot then still holds the data the graph
no longer has. If emptying
the store fails, the engine fail-stops, since the store then holds either the old data or nothing and later commits
must not be appended to data the graph no longer has. The graph calls `failStop()` itself when `TinkerStorage.clear`
throws, so this holds for an engine that does not stop itself, and the SPI has no default `clear`, so that no engine
inherits one built on a compaction that skips a store with nothing new in its log. A closed or fail-stopped graph refuses `clear()`, and a
recovered graph clears memory only.

Adding an element does not take the commit lock, so `clear()` empties memory one container at a time, marking each
before removing it. An element added meanwhile is either marked and removed, and its commit conflicts, or missed and
kept, and its commit lands in the emptied store. Either way memory and the store agree.

## Index Definitions

Indexed keys are recorded in `INDEXES` beside the store rather than in the log. An index changes how fast a lookup
runs, never its result, so a definition lost to a crash costs a rebuild and no data. An unreadable `INDEXES` file is
logged and the graph opens without indexes. Writes to the file are serialized under the commit lock and stop once the
graph is closed, recovered or fail-stopped.

## Settings Fixed at Creation

The storage engine, the three id managers, `defaultVertexPropertyCardinality`, `vertexLabelCardinality`,
`allowNullPropertyValues` and `preserveVertexPropertyIds` shape what is stored and how it is read back, so they are
recorded once and enforced on every open. `preserveVertexPropertyIds` is among them even though vertex records say
whether they carry vertex-property ids: every compaction rewrites the store under the setting it was opened with, and
`close()` compacts after any commit, so one open with the setting off would strip the ids from a store that kept them.
Settings that only affect how the engine runs (`sync`, `compactThreshold`, `recover`) may change between opens.
Cardinality is not recorded per property: replay applies the graph-wide default, as the `io()` step does.
