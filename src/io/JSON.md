## JSON IO Progress

This is a brain-dump of what we want to achieve with JSON IO.

### Reader

- [X] Read user schema
- [X] Support other numeric types other than `Float64, Int64`
- [X] Infer schema (basic)
- [X] Infer list schema
- [ ] Infer struct schema
- [ ] Coerce fields that have scalars and lists to lists
- [X] Support projection using field names
- [ ] Add option for dealing with case sensitivity
- [X] Coerce fields that can't be casted to provided schema (e.g. if one can't get int because of a float, convert the int to float instead of leaving null)
- [ ] Reduce repetition where possible
- [ ] Parity with CPP implementation (there's a Google Doc that has the spec)
- [ ] Add comprehensive tests
  - [X] Nulls at various places
  - [ ] *All* supported Arrow types
  - [X] Corrupt files and non-line-delimited files
  - [ ] Files where schemas don't match at all 
- [ ] Performance testing?