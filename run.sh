export RUST_BACKTRACE=full
export RUST_LOG=debug, aws_lock_client,dynamodb=debug
cargo run --bin=$1
