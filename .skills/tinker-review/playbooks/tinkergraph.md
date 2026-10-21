# Playbook: TinkerGraph

## Context
Changes under `tinkergraph-gremlin/`. TinkerGraph is the reference implementation, so
its behaviour is what users and providers assume Gremlin means. It comes in three
configurations, and each part of this playbook says which it applies to:
- `TinkerMemoryGraph` — in memory, no transactions, no storage. What `TinkerGraph.open()`
  returns.
- `TinkerStorageGraph` without a storage engine — in memory with transactions
  (optimistic, fail-fast on conflict); retains nothing across a restart.
- `TinkerStorageGraph` with a storage engine — the same transactions, with each commit
  written to a store directory before it is applied in memory and replayed on reopen.

Work out which configurations the changed code reaches before reading further: code
shared by both graph classes (elements, properties, indexes, id managers, the common
base) reaches all three; the transaction and commit path reaches both `TinkerStorageGraph`
configurations; storage engines and store files reach only the last.

Applies only when `meta.baseBranch` is `master`. On `3.7-dev` and `3.8-dev` TinkerGraph has
none of these configurations, so set this playbook aside there.

## Enrich
None applies beyond `general.md`; persistence-specific relationships are read from the
source during Inspect rather than written to the graph.

## Inspect
**All configurations:**
- Configuration — keys a configuration accepts but silently ignores. A key that asks for
  durability and is dropped turns persistence into data lost on restart.
- Features — what each configuration advertises (transactions, persistence) against
  what it does.
- Shared code — a change to element or graph code shared by both classes is a change to
  every configuration; check it was written for both, not only the one the PR targets.

**Transactional (`TinkerStorageGraph`, with or without a storage engine):**
- Element identity — what a caller holds versus what is committed. A reference obtained
  through an index, kept across a commit, or held across `clear()` must not be the shared
  committed object, or writes through it skip the transaction.
- Isolation — a write is visible to other threads only after commit and is undone by
  rollback, whatever path found the element (id lookup, index, edge endpoint).
- Concurrency — element locking fails fast with a `TransactionException`; check that new
  code neither blocks where it used to fail nor fails where it used to proceed.
- Commit path — everything a commit changes (elements, indexes, containers) is applied
  together or not at all, and cleanup after commit, rollback or `clear()` cannot drop
  another transaction's element.

**Storage engine (`TinkerStorageGraph` with a storage engine):**
- Integrity — every store file carries a check that detects damage, and every size the
  format declares is bounded before it is allocated.
- Crash points — at each step of a commit, compaction or `clear()`, the files left on
  disk reopen to a state the caller was told about. Atomic replacement (write, sync,
  rename) for whole files; nothing acknowledged is lost.
- Torn tail versus damage — the rule that discards an incomplete final record must not
  also discard a complete, acknowledged one that was damaged.
- Fail-stop — after an I/O failure the graph stops accepting commits, and a commit that
  became durable but was not applied in memory reports what it is.
- Locking — the directory lock holds against another process for the graph's whole
  life. Closing any descriptor on the lock file drops a POSIX lock; Windows locks are
  mandatory and behave differently.
- Recovery and settings — a recovery open writes nothing; settings that shape the data
  are fixed when the store is created.
- Interleavings — compaction against concurrent commits, `clear()` against open
  transactions.

## Verify
- Run the battery against every configuration the changed code reaches (see Context),
  not only the one the PR is about. Transactional scenarios (commit, rollback, a second
  thread's view) apply only to `TinkerStorageGraph`.
- With a storage engine, include a close and reopen in the same scenario: a change that
  is correct in memory can still persist the wrong state.
- Layer 1 (embedded, through the built Console); a fresh store directory per scenario.
- Open each configuration explicitly: `TinkerGraph.open()` is `TinkerMemoryGraph`;
  `TinkerStorageGraph.open()` has no storage engine; for a storage engine pass a
  configuration with `gremlin.tinkergraph.storage` and
  `gremlin.tinkergraph.storage.directory`.

With a storage engine, when the change reaches storage:
- Crash consistency — write, halt the JVM with `Runtime.halt` mid-operation, reopen in a
  new JVM, and compare what survives with what was acknowledged.
- Second process — two JVMs on one directory; the second must be refused for as long as
  the first is open. Confirm against the operating system's lock table (for example
  `/proc/locks`), not only the JVM's.
- Damage — corrupt each store file in turn (a flipped byte, a truncation, a file from
  another store) and check each is reported as damage rather than opened or silently
  cut.
- Recovery — a recovery open leaves every file byte-identical.

## Interpret
- A test that models another process with a second channel or graph in the same JVM — a
  test-quality finding: in-JVM locks and caches make it pass where two processes would
  not.
- A crash-window test whose assertion cannot fail for the reason it states — a
  test-quality finding.
- Durability, fsync or timing observations made on tmpfs — meaningless; a durability
  claim tested only there is untested.
- Do not propose running the standard structure, feature or computer suites with a
  storage engine (tp-kwk.41): they exercise Gremlin semantics, which storage does not
  change within an open graph.
- An isolation or identity defect found through a storage engine — high, and check
  `TinkerStorageGraph` without an engine too: the transactional code is shared, so the
  same bug usually exists there.
- A shared-code change tested against one configuration only — an open question per
  untested configuration it reaches.
- `checks.coverageGaps` on storage code — the tests that reach it are often
  fault-injection or reopen tests that the static call graph misses; read those before
  naming a gap.

## Escape
- if the change alters the on-disk format of an existing store with no version check or
  migration — "Format change without a version check: existing stores may open as damage or as wrong data"
