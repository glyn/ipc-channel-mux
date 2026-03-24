# Policies

## Design goals

Meet the design goals in README.md.

## Error handling

Propagate errors to the caller using `?` or by returning `Result`. Do not swallow errors by logging and continuing.

Do not use `unwrap()`, except in examples, tests, benchmarks, spawned threads and processes, and for unwrapping `MutexGuard`s. In spawned threads and processes use `expect()` rather than `unwrap()`.

## Encapsulation

Keep struct fields private.

## Visibility

Prefer `pub` over `pub(crate)`. `pub(crate)` grants visibility crate-wide
regardless of module hierarchy, which is broader than necessary for items that
are not part of the crate's public API. When an item's containing type or
module is not re-exported, `pub` is sufficient: the module system naturally
limits access to callers that can reach the module, without artificially
flattening visibility across the whole crate.

Use `pub(crate)` only when the containing module is publicly reachable but
the item should not be re-exported as part of the crate's public API.

## git history

Do not amend commits without confirming with the user.
