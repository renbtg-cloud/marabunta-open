// Marabunta - Licensed under the MIT License.
#[cfg(target_arch = "wasm32")]
extern "C" {
    pub fn mrb_fhe_add(a: u32, b: u32) -> u32;
    pub fn mrb_fhe_mul(a: u32, b: u32) -> u32;
    pub fn mrb_fhe_cmp_gt(a: u32, b: u32) -> u32;
    pub fn mrb_fhe_mux(cond: u32, a: u32, b: u32) -> u32;
}

#[cfg(not(target_arch = "wasm32"))]
pub unsafe fn mrb_fhe_add(_a: u32, _b: u32) -> u32 { 0 }
#[cfg(not(target_arch = "wasm32"))]
pub unsafe fn mrb_fhe_mul(_a: u32, _b: u32) -> u32 { 0 }
#[cfg(not(target_arch = "wasm32"))]
pub unsafe fn mrb_fhe_cmp_gt(_a: u32, _b: u32) -> u32 { 0 }
#[cfg(not(target_arch = "wasm32"))]
pub unsafe fn mrb_fhe_mux(_cond: u32, _a: u32, _b: u32) -> u32 { 0 }
