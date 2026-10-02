// Marabunta - Licensed under the MIT License.
use proc_macro::TokenStream;
use quote::quote;
use syn::fold::Fold;
use syn::{parse_macro_input, parse_quote, Expr, ItemFn};

struct BlindFolder;

impl Fold for BlindFolder {
    fn fold_expr(&mut self, expr: Expr) -> Expr {
        // First fold the inner expressions
        let expr = syn::fold::fold_expr(self, expr);

        match expr {
            Expr::Binary(bin) => {
                let op_ident = match bin.op {
                    syn::BinOp::Add(_) => Some(quote!(mrb_fhe_add)),
                    syn::BinOp::Mul(_) => Some(quote!(mrb_fhe_mul)),
                    syn::BinOp::Gt(_) => Some(quote!(mrb_fhe_cmp_gt)),
                    _ => None,
                };

                if let Some(op) = op_ident {
                    let left = bin.left;
                    let right = bin.right;
                    let call: Expr = parse_quote! {
                        unsafe { crate::ffi::#op(#left, #right) }
                    };
                    return call;
                }
                Expr::Binary(bin)
            }
            Expr::If(if_expr) => {
                if let Some((_, else_branch)) = if_expr.else_branch {
                    let cond = if_expr.cond;
                    let then_branch = if_expr.then_branch;
                    
                    let call: Expr = parse_quote! {
                        unsafe { crate::ffi::mrb_fhe_mux(#cond, #then_branch, #else_branch) }
                    };
                    return call;
                }
                Expr::If(if_expr)
            }
            _ => expr,
        }
    }
}

pub fn expand_blind(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);
    let mut folder = BlindFolder;
    let folded = folder.fold_item_fn(input);
    TokenStream::from(quote! { #folded })
}
