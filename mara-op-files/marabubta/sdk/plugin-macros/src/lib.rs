// Marabunta - Licensed under the MIT License.
mod blind;
use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

/// Generates WASM entry points for a Marabunta plugin.
///
/// Annotate your plugin struct with `#[marabunta_plugin]` and implement
/// `ComputePlugin` for it. The macro generates:
///
/// - `__marabunta_execute` — the WASM entry point called by the sandbox
/// - `__marabunta_alloc` / `__marabunta_dealloc` — memory management exports
/// - A panic handler mapping panics to `PluginError::Internal`
///
/// These exports are only generated when compiling for `wasm32`.
/// On other targets, the macro is a no-op passthrough.
#[proc_macro_attribute]
pub fn marabunta_plugin(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    let name = &input.ident;

    let expanded = quote! {
        #input

        #[cfg(target_arch = "wasm32")]
        mod __marabunta_generated {
            use super::*;

            #[no_mangle]
            pub extern "C" fn __marabunta_alloc(size: u32) -> *mut u8 {
                let layout = std::alloc::Layout::from_size_align(size as usize, 1).unwrap();
                unsafe { std::alloc::alloc(layout) }
            }

            #[no_mangle]
            pub extern "C" fn __marabunta_dealloc(ptr: *mut u8, size: u32) {
                let layout = std::alloc::Layout::from_size_align(size as usize, 1).unwrap();
                unsafe { std::alloc::dealloc(ptr, layout) }
            }

            #[no_mangle]
            pub extern "C" fn __marabunta_execute(
                input_ptr: *const u8,
                input_len: u32,
                params_ptr: *const u8,
                params_len: u32,
                output_ptr: *mut *mut u8,
                output_len: *mut u32,
            ) -> i32 {
                let input = unsafe {
                    std::slice::from_raw_parts(input_ptr, input_len as usize)
                };
                let params = unsafe {
                    std::slice::from_raw_parts(params_ptr, params_len as usize)
                };

                let plugin = #name;
                match marabunta_plugin_sdk::ComputePlugin::execute(&plugin, input, params) {
                    Ok(result) => {
                        let boxed = result.into_boxed_slice();
                        let len = boxed.len();
                        let ptr = Box::into_raw(boxed) as *mut u8;
                        unsafe {
                            *output_ptr = ptr;
                            *output_len = len as u32;
                        }
                        0
                    }
                    Err(e) => {
                        match e {
                            marabunta_plugin_sdk::PluginError::InvalidInput => 1,
                            marabunta_plugin_sdk::PluginError::InvalidParams => 2,
                            marabunta_plugin_sdk::PluginError::ResourceExhausted => 3,
                            marabunta_plugin_sdk::PluginError::Internal(_) => 4,
                        }
                    }
                }
            }
        }
    };

    TokenStream::from(expanded)
}
#[proc_macro_attribute]
pub fn blind(_attr: TokenStream, item: TokenStream) -> TokenStream {
    blind::expand_blind(item)
}
