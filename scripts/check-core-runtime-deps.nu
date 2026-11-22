# Check Core's dependency graph without runtime features from other workspace packages.
let tree = do --capture-errors {
    ^cargo tree --locked -p flotsync_core --no-default-features --edges normal,build --target all --prefix none --format '{p}' --color never
}

let runtime_deps = $tree
    | parse --regex '(?m)^(?<name>async-std|kompact) '
    | get name
    | uniq

if ($runtime_deps | is-not-empty) {
    let packages = $runtime_deps | str join ', '
    error make --unspanned {msg: $"flotsync_core pulls in runtime dependencies: ($packages)"}
}

^cargo check --locked -p flotsync_core --no-default-features --lib
