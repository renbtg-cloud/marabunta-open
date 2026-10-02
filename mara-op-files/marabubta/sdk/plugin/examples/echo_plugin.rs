// Marabunta - Licensed under the MIT License.
//! Echo plugin -- the simplest possible Marabunta computation plugin.
//!
//! Returns the input bytes unchanged. Useful as a starting template and for
//! round-trip integration tests.

use marabunta_plugin_sdk::prelude::*;

// ---------------------------------------------------------------------------
// Plugin definition
// ---------------------------------------------------------------------------

/// A zero-logic plugin that echoes its input verbatim.
#[marabunta_plugin]
#[derive(Default)]
struct EchoPlugin;

impl ComputePlugin for EchoPlugin {
    fn execute(&self, input: &[u8], _params: &[u8]) -> Result<Vec<u8>, PluginError> {
        Ok(input.to_vec())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use marabunta_plugin_sdk::testing::TestHarness;

    #[test]
    fn echo_returns_input_unchanged() {
        let harness = TestHarness::builder()
            .jurisdiction(CountryCode::DE)
            .zone_class(ZoneClass::Civilian)
            .classification(ClassificationLevel::Unclassified)
            .build();

        let input = b"hello, marabunta!";
        let result = harness
            .execute::<EchoPlugin>(input, b"")
            .expect("execution should succeed");

        assert_eq!(result, input);
    }

    #[test]
    fn echo_handles_empty_input() {
        let harness = TestHarness::builder().build();

        let result = harness
            .execute::<EchoPlugin>(b"", b"")
            .expect("empty input should succeed");

        assert!(result.is_empty());
    }

    #[test]
    fn echo_ignores_params() {
        let harness = TestHarness::builder().build();

        let input = b"data";
        let params = b"these params are ignored";
        let result = harness
            .execute::<EchoPlugin>(input, params)
            .expect("execution should succeed");

        assert_eq!(result, input);
    }
}

// ---------------------------------------------------------------------------
// Binary entry point (required for `cargo run --example echo_plugin`)
// ---------------------------------------------------------------------------

fn main() {}
