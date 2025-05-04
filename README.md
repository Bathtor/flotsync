# FlotSync

**FlotSync** is a peer-to-peer group synchronization library written in Rust. It provides a decentralized protocol for synchronizing state between multiple nodes in a group, without relying on a central authority.

## Getting Started

### Requirements

- [Rust](https://www.rust-lang.org/tools/install) (latest stable version recommended)

### Installation

Add flotsync to your `Cargo.toml`:

```toml
[dependencies]
flotsync = { git = "https://github.com/Bathtor/flotsync.git" }
```

### Running Tests

To run all tests:

```bash
cargo test
```

## Project Structure

-	`flotsync_core/` — Core synchronization logic and types

## Contributing

Contributions, issues, and feature requests are welcome!

1.	Fork the repo
2.	Create a feature branch (`git checkout -b my-feature`)
3.	Commit your changes (`git commit -am 'Add new feature'`)
4.	Push to the branch (`git push origin my-feature`)
5.	Create a Pull Request

## License

This project is licensed under the MIT License. See the LICENSE file for details.
