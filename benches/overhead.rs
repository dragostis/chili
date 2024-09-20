use chili::{Scope, ThreadPool};
use divan::Bencher;

struct Node {
    val: u64,
    left: Option<Box<Node>>,
    right: Option<Box<Node>>,
}

impl Node {
    pub fn tree(layers: usize) -> Self {
        Self {
            val: 1,
            left: (layers != 1).then(|| Box::new(Self::tree(layers - 1))),
            right: (layers != 1).then(|| Box::new(Self::tree(layers - 1))),
        }
    }
}

const LAYERS: &[usize] = &[10, 24];
fn nodes() -> impl Iterator<Item = (usize, usize)> {
    LAYERS.iter().map(|&l| (l, (1 << l) - 1))
}

#[divan::bench(args = nodes())]
fn no_overhead(bencher: Bencher, nodes: (usize, usize)) {
    fn join_no_overhead<A, B, RA, RB>(scope: &mut Scope<'_>, a: A, b: B) -> (RA, RB)
    where
        A: FnOnce(&mut Scope<'_>) -> RA + Send,
        B: FnOnce(&mut Scope<'_>) -> RB + Send,
        RA: Send,
        RB: Send,
    {
        (a(scope), b(scope))
    }

    #[inline]
    fn sum(node: &Node, scope: &mut Scope<'_>) -> u64 {
        let (left, right) = join_no_overhead(
            scope,
            |s| node.left.as_deref().map(|n| sum(n, s)).unwrap_or_default(),
            |s| node.right.as_deref().map(|n| sum(n, s)).unwrap_or_default(),
        );

        node.val + left + right
    }

    let tree = Node::tree(nodes.0);
    let thread_pool = ThreadPool::new().unwrap();
    let mut scope = thread_pool.scope();

    bencher.bench_local(move || {
        assert_eq!(sum(&tree, &mut scope), nodes.1 as u64);
    });
}

#[divan::bench(args = nodes())]
fn chili_overhead(bencher: Bencher, nodes: (usize, usize)) {
    fn sum(node: &Node, scope: &mut Scope<'_>) -> u64 {
        let (left, right) = scope.join(
            |s| node.left.as_deref().map(|n| sum(n, s)).unwrap_or_default(),
            |s| node.right.as_deref().map(|n| sum(n, s)).unwrap_or_default(),
        );

        node.val + left + right
    }

    let tree = Node::tree(nodes.0);
    let thread_pool = ThreadPool::new().unwrap();
    let mut scope = thread_pool.scope();

    bencher.bench_local(move || {
        assert_eq!(sum(&tree, &mut scope), nodes.1 as u64);
    });
}

#[divan::bench(args = nodes())]
fn rayon_overhead(bencher: Bencher, nodes: (usize, usize)) {
    fn sum(node: &Node) -> u64 {
        let (left, right) = rayon::join(
            || node.left.as_deref().map(sum).unwrap_or_default(),
            || node.right.as_deref().map(sum).unwrap_or_default(),
        );

        node.val + left + right
    }

    let tree = Node::tree(nodes.0);

    bencher.bench_local(move || {
        assert_eq!(sum(&tree), nodes.1 as u64);
    });
}

fn main() {
    divan::main();
}
