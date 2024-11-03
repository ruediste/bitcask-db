# Bitcask DB

A DB inspired by the [Bitcask](https://riak.com/assets/bitcask-intro.pdf) paper.

# Data Structures

absent offsets: -1

## Log Files

A log file is just a sequence of entries. Each entry is a key-value pair.

Log entry:

- key size
- value size
- key data
- value data

A value size of -1 represents a tombstone.

## Index File

The index is a hash table, based on the key hashes. When the index is created, the number of buckets is already known, so there is no need for rehashing.

- uint32: number of buckets
- per Bucket:
  - chain offset (optional)
  - 4 times
    - key hash
    - offset
- per chain:
  - previous chain offset (optional)
  - 4 times
    - key hash
    - offset

Note that in the case of hash collisions, the same key hash can appear multiple times in the same bucket. But there can only be a single entry per key at any given time. For deleted keys, there is still an entry in the index, pointing to the tombstone.

## Current Segment

Normal log files are used for the current segment. The Index is kept in memory as an unordered multimap from hash to offsets in the log file. When the current segment reaches a certain size, a new current segment is created. An index file for the old current is created. During this time, the old in-memory index is still used.

# Algorithms

## Insert

An entry is appended to the current log file. All entries with the key hash are examined. If an entry for the same key is found, it is replaced with the new offset. Otherwise an entry is added.

## Delete

Same as insert, but with a tombstone.

## Find

All entries with the same key hash are examined, starting with the current segment and continuing the other segments, newest to oldest. If an entry or tombstone is found, it is returned.

## Compaction

For compaction, we always compact the two adjacent segments with the smallest combined size. => Algorithm to find segments to be combined to be determined.

A new log file and an new index file is created for the combined segment. For each segment, the log is read sequentially. For each entry, a lookup into the whole database is performed. If the entry is the latest one for the key, it is kept, otherwise it is skipped. This applies to both normal entries and tombstones. If the first segment is being compacted, all tombstones are skipped, since the original entries will have been skipped already, so there is no need for a tombstone.

In addition to the log file, a second temporary file is created, containing just the key hashes and the corresponding offsets.

During the log construction, the number of entries is counted. This allows to create the index file with the right number of buckets from the start. The index is then created using the temporary key hash/offset file. The temporary file is then deleted.
