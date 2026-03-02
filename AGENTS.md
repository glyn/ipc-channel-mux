# Policies

## Design goals

Meet the design goals in README.md.

## Error handling

The code should return errors rather than use unwrap(), except in examples, tests, benchmarks, spawned threads and processes, and for unwrapping MutexGuards.

In spawned threads and processes use except() rather than unwrap().

Do not swallow errors, log them using log::debug!().

## Encapsulation

Keep struct fields private.
