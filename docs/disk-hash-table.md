# Disk-Backed Hash Table

Surprisingly, there is no common way to store a giant dictionary of
hashes on disk for use with `mmap`. There is a brief explanation of
someone [rolling their own solution in perl](https://stackoverflow.com/a/509179/1405768).
There is also [KISSDB](https://github.com/adamierymenko/kissdb), which
is not really for this situation. Here are the goals of the format:

* Optimize for write-once-read-many workflow. These tables will be
  created from data in CSVs.
* Rely on the high entropy of good hash algorithms. There is no need
  to prepare for a pathologically large number of hashes ending up
  in the same bucket.
* When it is not cumbersome, avoid storing redundant data. More
  concretely, this means using prefix compression at the byte level
  but not the bit level.

A disk-backed hash table shall use the extension `.hsht`. All numbers
represented in the file are in network byte order (big-endian). The
file begins with a header. All fields in the header are 32-bit unsigned
integers.

* Identifier: The 32-bit constant `0xb4a10963`.
* Key Size (K): Number of bytes in a key. This is likely 16 (SHA1 and MD5),
  32 (SHA-256), or 64 (SHA-512). Load-time validation checks should
  reject databases with unexpected key sizes. We will use `K_bits` to
  refer to the key size as bits instead of bytes. `K_bits = K * 8`.
* Bucketing Bits (B): How many leading bits in each key are used for
  bucketing. Invariant: `B <= K_bits`.
* Effective Key Size (KF): How many bytes of each key are stored in
  the data table. Invariant: `KF >= K - (B / 8)_truncated`. A table
  that does not perform any prefix compression will have `KF = K`.
* Offset Size (F): Number of bytes used to represent the offsets that
  are the values of the prefix table. This may not be larger than 8.
* Value Size (V): Number of bytes in the value associated with each key.
  It is allowed for this to be zero. Unlike in general-purpose databases,
  values are fixed-width.
* Data Offset (DOFF): The offset of the data table from the root pointer.
  Invariant: `DOFF >= 32 + (((2 ^ B) + 1) * F)`. Typically, this will
  be set to the lowest possible value. However, if prefix compression
  is not used and if keys are size 16, 32, or 64, and if values are size 0
  (or if they are the same size as the keys), a database writer may want
  to round `DOFF` up to a multiple of `K`. A clever reader could take
  advantage of this situation by loading keys from the data table into
  AVX registers.
* Reserved: The 32-bit constant `0x00000000`. This pads the header to
  exactly 32 bytes.

After this 32-byte header, there is the prefix table. The size of the
prefix table, in bytes, is given as `((2 ^ B) + 1) * F`. The prefix table
is an associative array. Assuming `B = 3` and `F = 5`, the prefix table
might look like this:

    (prefix 0b000) 0000000000
    (prefix 0b001) 0000000005
    (prefix 0b002) 0000000007
    (prefix 0b003) 0000000021
    (prefix 0b004) 000000002C
    (prefix 0b005) 0000000109
    (prefix 0b006) 0000000109 (Repeated offset implies an empty previous bucket)
    (prefix 0b007) 00000001F9
    (trailing off) 00000001FB (Trailing offset used to indicate size of last bucket)

The keys of the prefix table are B-bit prefixes. They are implicit in the
position of the values. The values are 2-tuples. The first element (always
one byte) is total number of keys that matched this prefix. The second
element (represented by F bytes) is the offset into the data table (in number
of data entries) where the keys that matched this prefix begin. Notice that
in the example above, the offset size was much too large. It was chosen to
be 5, but a more intelligent builder would have chosen 2 instead.

After the prefix table, there may be some unused bytes before `DOFF` is
reached. These should all be set to zero. Then the data table starts.
Below is an example data table that corresponds to the above prefix table.
We assume `K = 8` and `V = 2`:

    0A1BDCE2|ABAB
    073848A6|1234
    1127F091|CDCD   Leading bits are 000
    1845A11F|789A
    1F043B90|2345
    --------+----
    30A1FD09|5240   Leading bits are 001
    4AB54618|7641
    --------+----
    5F1C0FDA|F111
    6B0A12BC|B041   Leading bits are 002
    ...     |...

This did not show up in this example because the prefix size was less than
eight, but generally, leading bytes from each key are removed. The number
of leading bytes to remove is given by `B / 8` (truncated), so when `B = 3`,
as above, this is zero. Notice that the leading bits in each key in the
example above agree with the bucket they are in. Also, the key-value pairs
appear in sorted order in each bucket. After being looked up by a prefix, the
entire bucket must be scanned to check for matches. For sufficiently large
bucket, a well-designed reader may perform binary search instead, but this
optimization is not required. One more important thing to notice is that the
buckets *must* appear in the order of their prefixes. Here are a few more
calculations for pointers to the tables:

* `PREFIX = ROOT + 32`
* `DATA = ROOT + DOFF`
* `BKT_eff = KF + V` (effective key-value pair size)

When implementing a database reader, these numbers should be computed
once at load time. Then, `lookup` is given by the pseudocode:

    -- Square brackets indicate indexing with byte offsets,
    -- not element offsets.
    lookup(key) {
      bucket = most_significant_bits(key,B)
      prefix_row = PREFIX + (bucket * F)
      next_prefix_row = prefix_row + F
      bucket_off = prefix_row[start 0, len F] -- slice, probably done with a loop, MSB-encoded number
      next_bucket_off = next_prefix_row[start 0, len F] -- similar loop
      bucket_elements = next_bucket_off - bucket_off
      data_row = DATA + (bucket_off * BKT_eff)
      needle = key[start (K - KF), len KF] -- drop prefix from key
      for (i = bucket_elements - 1; i > (-1); i = i - 1) {
        haystack = data_row[start 0, len KF]
        if needle == haystack {
          val = data_row[start KF, len V];
          return val
        }
        data_row = data_row + BKT_eff;
      }
      return NULL
    }

It is also possible to specialize the code above for 0-length values,
resulting in a hash set where a membership test returns true or false.
