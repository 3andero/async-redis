use proc_macro::TokenStream;
use quote::quote;
use syn::parse::Parser;
use syn::{parse, parse_macro_input, ItemStruct};

macro_rules! vector_push {
    ($fields:ident, $($name:ident: $type:ty),*) => {
        $(
            $fields.named.push(
                syn::Field::parse_named
                    .parse2(quote! { pub $name: $type }).unwrap(),
            );
        )*
    }
}

#[proc_macro_attribute]
pub fn define_traverse_command(args: TokenStream, input: TokenStream) -> TokenStream {
    let mut item_struct = parse_macro_input!(input as ItemStruct);
    let branch = parse_macro_input!(args as syn::LitStr)
        .value()
        .to_lowercase();

    if let syn::Fields::Named(ref mut fields) = item_struct.fields {
        match branch.as_bytes() {
            b"n:1" => {
                vector_push!(
                    fields,
                    db_amount: usize,
                    cmds: Vec<MiniCommand>,
                    cmds_tbl: Vec<Vec<MiniCommand>>,
                    len: usize
                );
            }
            b"n:n" => {
                vector_push!(
                    fields,
                    db_amount: usize,
                    cmds: Vec<MiniCommand>,
                    cmds_tbl: Vec<Vec<MiniCommand>>,
                    order_tbl: Vec<Vec<usize>>,
                    len: usize
                );
            }
            _ => panic!(),
        }
    }

    return quote! {
        #item_struct
    }
    .into();
}
