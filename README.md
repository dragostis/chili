# spice-rs

## Rust port of [Spice], a low-overhead parallelization library

Very low-overhead parallelization primitive, almost identical to
[`rayon::join`]. At any fork point during computation, it *may* run the two
passed closures in parallel.

It works best in cases where there are many small computations and where it is
expensive to estimate how many are left on the current branch in order to stop trying to share work across threads.

## Example

The following example sums up all nodes in a binary tree in parallel.

```rust
fn sum(node: &Node, scope: &mut Scope<'_>) -> u64 {
    let (left, right) = scope.join(
        |s| node.left.as_deref().map(|n| sum(n, s)).unwrap_or_default(),
        |s| node.right.as_deref().map(|n| sum(n, s)).unwrap_or_default(),
    );

    node.val + left + right
}
```

This is the ideal example since per-node computation is very cheap and the
nodes don't keep track of how many descendants are left.

## Benchmarks

The following benchmarks measure the time it takes to sum up all the values in
a balanced binary tree with varying number of nodes.

### AMD Ryzen 7 4800HS (8 cores)

While the improvement over the baseline in the 134M nodes case is close to the
theoretical maximum, it's worth noting that the actual time per node is 0.8ns
vs. a theoretical 1.8 / 8 = 0.2ns, if we're to compare against the 1K nodes
case.

| Number of nodes | Baseline |  Rayon   | spice-rs | Baseline / spice-rs |
|----------------:|---------:|---------:|---------:|:-------------------:|
|            1023 |   1.8 µs |  51.1 µs |   3.4 µs |      **x0.53**      |
|        16777215 |  94.4 ms |  58.1 ms |  13.6 ms |      **x6.94**      |
|       134217727 | 797.5 ms | 497.2 ms | 101.8 ms |      **x7.83**      |

### Apple M1 (8 cores)

| Number of nodes | Baseline |  Rayon   | spice-rs | Baseline / spice-rs |
|----------------:|---------:|---------:|---------:|:-------------------:|
|            1023 |   1.6 µs |  29.2 µs |   3.5 µs |      **x0.46**      |
|        16777215 |  39.4 ms |  40.5 ms |  11.2 ms |      **x3.51**      |
|        67108863 | 156.5 ms | 167.1 ms |  44.3 ms |      **x3.53**      |

### spice-rs overhead on AMD Ryzen 7 4800HS (8 cores)

The oveerhead in the 1K nodes case remains approximately constant with respect
to the number of threads.

| Number of nodes | Baseline | 1 thread | 2 threads | 4 threads | 8 threads |
|----------------:|---------:|---------:|----------:|----------:|----------:|
|            1023 |   1.8 ns |   3.5 ns |    3.5 µs |    3.5 µs |    3.5 µs |

[Spice]: https://github.com/judofyr/spice
[`rayon::join`]: https://docs.rs/rayon/latest/rayon/fn.join.html