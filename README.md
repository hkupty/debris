# debris

---

Debris is an alpha-state serialization format that does *not* focus on speed or size.

Its focus is on being deterministic, simple and predictable.

It should write binary data in a way that:

  - No setup is required;
  - Any implementation generates the same result;
  - Unordered collections are serialized in a sorted manner;
  - Data is never lost, but the data types can be widened on de-serialization.

In addition to usual scenarios for serializing data, this properties would allow any data serialized in debris to:
  - Produce reliable hashes or signatures;
  - Diff against other serialized chunk for updates;

## Layout

Every data type in debris is serialized according to the same schema:

```
  |   1 byte  ||   4 bytes   ||  ...  |
  [type header][size in bytes][payload]
```

This means that:

  - The top-level collection can hold up to 4GB of data;
  - Collections can hold heterogeneous data;
  - Strings are serialized in utf-8;
  - Numbers are serialized a as Decimal representation;

The types are:

| Header Prefix | Data type | Obs |
|---|---|---|
| `0x00` | Byte/Bytes  |   |
| `0x01` | Boolean |   |
| `0x02` | Number | Serialized as Decimal |
| `0x03` | Text | Serialized as UTF-8 |
| `0x10` | Unordered Sequence | Sorted before serialized |
| `0x11` | Unordered Map | Sorted by key before serialized |
| `0x20` | Ordered Sequence |   |
| `0x21` | Ordered Map |   |

To sort `0x10` and `0x11`, the header is compared first and then the payload:
```clojure
;; Using commas to improve readability
(debris/serialize {true 10 false 20})

;; 1: [true 10], 2: [false 20]
;; 1: [[0x01,0x01,0x01] [0x02,0x02,0x31,0x30]] 2: [[0x01,0x01,0x00] [0x02,0x02,0x32,0x30]]
;;
;; Final serialization:
;; |---map---|----------false 20---------------|-------------true 10-------------------|
;; [0x11,0x0E,0x01,0x01,0x0,0x02,0x02,0x32,0x30,0x01,0x01,0x01,0x01,0x02,0x02,0x31,0x30]
```
