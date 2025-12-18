// Expose our generated code to other crates in the workspace.
// https://doc.rust-lang.org/reference/items/modules.html#the-path-attribute
#[path = "collector/collector.rs"]
pub mod collector;
