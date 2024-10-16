extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_derive(Columnar)]
pub fn derive(input: TokenStream) -> TokenStream {

    let ast = parse_macro_input!(input as DeriveInput);
    let name = &ast.ident;

    match ast.data {
        syn::Data::Struct(data_struct) => {
            derive_struct(name, &ast.generics, data_struct)
        }
        syn::Data::Enum(data_enum) => {
            derive_enum(name, &ast.generics, data_enum)
        }
        _ => unimplemented!(),
    }
}

fn derive_struct(name: &syn::Ident, generics: &syn:: Generics, data_struct: syn::DataStruct) -> proc_macro::TokenStream {

    let c_name = format!("{}Container", name);
    let c_ident = syn::Ident::new(&c_name, name.span());

    let r_name = format!("{}Reference", name);
    let r_ident = syn::Ident::new(&r_name, name.span());

    let named = match &data_struct.fields {
        syn::Fields::Named(_) => true,
        syn::Fields::Unnamed(_) => false,
        _ => unimplemented!(),
    };

    let names: &Vec<_> = &match &data_struct.fields {
        syn::Fields::Named(fields) => fields.named.iter().map(|field| field.ident.clone().unwrap()).collect(),
        syn::Fields::Unnamed(fields) => (0 .. fields.unnamed.len()).map(|index| syn::Ident::new(&format!("f{}", index), name.span())).collect(),
        _ => unimplemented!(),
    };

    let types: &Vec<_> = &match &data_struct.fields {
        syn::Fields::Named(fields) => fields.named.iter().map(|field| &field.ty).collect(),
        syn::Fields::Unnamed(fields) => fields.unnamed.iter().map(|field| &field.ty).collect(),
        _ => unimplemented!(),
    };

    // Generic type parameters for the containers for the struct fields.
    let container_types = &names.iter().enumerate().map(|(index, name)| {
        let new_name = format!("C{}", index);
        syn::Ident::new(&new_name, name.span())
    }).collect::<Vec<_>>();

    // The container struct is a tuple of containers, named to correspond with fields.
    let container_struct = {
        quote! {
            #[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
            pub struct #c_ident < #(#container_types),* >{
                #(pub #names : #container_types, )*
            }
        }
    };

    let reference_struct = {

        let reference_types = &names.iter().enumerate().map(|(index, name)| {
            let new_name = format!("R{}", index);
            syn::Ident::new(&new_name, name.span())
        }).collect::<Vec<_>>();
    
        let ty_gen = quote! { < #(#reference_types),* > };

        quote! {
            #[derive(Copy, Clone, Debug)]
            pub struct #r_ident #ty_gen {
                #(pub #names : #reference_types, )*
            }
        }
    };

    let partial_eq = {

        let reference_types = &names.iter().enumerate().map(|(index, name)| {
            let new_name = format!("R{}", index);
            syn::Ident::new(&new_name, name.span())
        }).collect::<Vec<_>>();

        let (_impl_gen, ty_gen, _where_clause) = generics.split_for_impl();

        let struct_generics = generics.params.iter();
        let impl_gen = quote! { < #(#struct_generics,)* #(#reference_types),* > };

        let where_clause = quote! { where #(#reference_types: PartialEq<#types>),* };

        // Either use curly braces or parentheses to destructure the item.
        let destructure_self =
        if named { quote! { let #name { #(#names),* } = other; } }
        else     { quote! { let #name ( #(#names),* ) = other; } };

        quote! {
            impl #impl_gen PartialEq<#name #ty_gen> for #r_ident < #(#reference_types),* >  #where_clause {
                fn eq(&self, other: &#name #ty_gen) -> bool {
                    #destructure_self
                    #(self.#names == *#names) &&*
                }
            }
        }

    };

    let push_own = { 
        let (_impl_gen, ty_gen, _where_clause) = generics.split_for_impl();
        let push = names.iter().map(|name| { quote! { self.#name.push(#name); } });
        
        let struct_generics = generics.params.iter();
        let impl_gen = quote! { < #(#struct_generics,)* #(#container_types),* > };

        let where_clause2 = quote! { where #(#container_types: ::columnar::Push<#types>),* };

        // Either use curly braces or parentheses to destructure the item.
        let destructure_self = 
        if named { quote! { let #name { #(#names),* } = item; } }
        else     { quote! { let #name ( #(#names),* ) = item; } };

        quote! {
            impl #impl_gen ::columnar::Push<#name #ty_gen> for #c_ident < #(#container_types),* >  #where_clause2 {
                fn push(&mut self, item: #name #ty_gen) {
                    #destructure_self
                    #(#push)*
                }
            }
        }
    };

    let push_ref = { 
        let (_impl_gen, ty_gen, _where_clause) = generics.split_for_impl();
        let push = names.iter().map(|name| { quote! { self.#name.push(#name); } });
        
        let struct_generics = generics.params.iter();
        let impl_gen = quote! { < 'columnar, #(#struct_generics,)* #(#container_types),* > };

        let where_clause2 = quote! { where #(#container_types: ::columnar::Push<&'columnar #types>),* };

        let destructure_self = 
        if named { quote! { let #name { #(#names),* } = item; } }
        else     { quote! { let #name ( #(#names),* ) = item; } };

        quote! {
            impl #impl_gen ::columnar::Push<&'columnar #name #ty_gen> for #c_ident < #(#container_types),* >  #where_clause2 {
                fn push(&mut self, item: &'columnar #name #ty_gen) {
                    #destructure_self
                    #(#push)*
                }
            }
        }
    };

    // Implementation of `Push<#r_ident>`
    let push_new = { 

        let reference_types = &names.iter().enumerate().map(|(index, name)| {
            let new_name = format!("R{}", index);
            syn::Ident::new(&new_name, name.span())
        }).collect::<Vec<_>>();

        let push = names.iter().map(|name| { quote! { self.#name.push(#name); } });
        
        let impl_gen = quote! { < #(#container_types,)* #(#reference_types),* > };

        let where_clause = quote! { where #(#container_types: ::columnar::Push<#reference_types>),* };

        let index_type = quote! { #r_ident < #(#reference_types,)* > };
        let destructure_self = quote! { let #r_ident { #(#names),* } = item; };

        quote! {
            impl #impl_gen ::columnar::Push<#index_type> for #c_ident < #(#container_types),* > #where_clause {
                fn push(&mut self, item: #index_type) {
                    #destructure_self
                    #(#push)*
                }
            }
        }
    };

    let index_own = {
        let impl_gen = quote! { < #(#container_types),* > };
        let ty_gen = quote! { < #(#container_types),* > };
        let where_clause = quote! { where #(#container_types: ::columnar::Index),* };

        let index_type = quote! { #r_ident < #(<#container_types as ::columnar::Index>::Ref,)* > };

        quote! {
            impl #impl_gen ::columnar::Index for #c_ident #ty_gen #where_clause {
                type Ref = #index_type;
                fn get(&self, index: usize) -> Self::Ref {
                    #r_ident { #(#names: self.#names.get(index),)* }
                }
            }
        }
    };

    let index_ref = {
        let impl_gen = quote! { < 'columnar, #(#container_types),* > };
        let ty_gen = quote! { < #(#container_types),* > };
        let where_clause = quote! { where #(&'columnar #container_types: ::columnar::Index),* };

        let index_type = quote! { #r_ident < #(<&'columnar #container_types as ::columnar::Index>::Ref,)* > };

        quote! {
            impl #impl_gen ::columnar::Index for &'columnar #c_ident #ty_gen #where_clause {
                type Ref = #index_type;
                fn get(&self, index: usize) -> Self::Ref {
                    #r_ident { #(#names: (&self.#names).get(index),)* }
                }
            }
        }
    };

    let clear = { 

        let impl_gen = quote! { < #(#container_types),* > };
        let ty_gen = quote! { < #(#container_types),* > };
        let where_clause = quote! { where #(#container_types: ::columnar::Clear),* };

        quote! {
            impl #impl_gen ::columnar::Clear for #c_ident #ty_gen #where_clause {
                fn clear(&mut self) { #(self.#names.clear());* }
            }
        }
    };

    let length = { 

        let impl_gen = quote! { < #(#container_types),* > };
        let ty_gen = quote! { < #(#container_types),* > };
        let where_clause = quote! { where #(#container_types: ::columnar::Len),* };

        let first_name = &names[0];
        
        quote! {
            impl #impl_gen ::columnar::Len for #c_ident #ty_gen #where_clause {
                fn len(&self) -> usize {
                    self.#first_name.len()
                }
            }
        }
    };

    let as_bytes = { 

        let impl_gen = quote! { < #(#container_types),* > };
        let ty_gen = quote! { < #(#container_types),* > };
        let where_clause = quote! { where #(#container_types: ::columnar::bytes::AsBytes),* };
        
        quote! {
            impl #impl_gen ::columnar::bytes::AsBytes for #c_ident #ty_gen #where_clause {
                type Borrowed<'columnar> = #c_ident < #(<#container_types as ::columnar::bytes::AsBytes>::Borrowed<'columnar>,)*>;
                fn as_bytes(&self) -> impl Iterator<Item=(u64, &[u8])> {
                    let iter = None.into_iter();
                    #( let iter = iter.chain(self.#names.as_bytes()); )*
                    iter
                }
            }
        }
    };

    let from_bytes = { 

        let impl_gen = quote! { < 'columnar, #(#container_types),* > };
        let ty_gen = quote! { < #(#container_types),* > };
        let where_clause = quote! { where #(#container_types: ::columnar::bytes::FromBytes<'columnar>),* };
        
        quote! {
            impl #impl_gen ::columnar::bytes::FromBytes<'columnar> for #c_ident #ty_gen #where_clause {
                fn from_bytes(bytes: &mut impl Iterator<Item=&'columnar [u8]>) -> Self {
                    #(let #names = ::columnar::bytes::FromBytes::from_bytes(bytes);)*
                    Self { #(#names,)* }
                }
            }
        }
    };

    let columnar_impl = {

        let (impl_gen, ty_gen, where_clause) = generics.split_for_impl();

        let where_clause2 = if let Some(struct_where) = where_clause {
            let params = struct_where.predicates.iter(); 
            quote! {  where #(#types : ::columnar::Columnar,)* #(#params),* }
        }
        else {
            quote! { where #(#types : ::columnar::Columnar,)* }
        };

        quote! {
            impl #impl_gen ::columnar::Columnar for #name #ty_gen #where_clause2 {
                type Container = #c_ident < #(<#types as ::columnar::Columnar>::Container ),* >;
            }
        }
    };


    quote! {

        #container_struct
        #reference_struct

        #partial_eq

        #push_own
        #push_ref
        #push_new

        #index_own
        #index_ref
        #length
        #clear

        #as_bytes
        #from_bytes

        #columnar_impl

    }.into()
}

/// The derived container for an `enum` type will be a struct with containers for each field of each variant, plus an offset container and a discriminant container.
/// Its index `Ref` type will be an enum with parallel variants, each containing the index `Ref` types of the corresponding variant containers.
#[allow(unused)]
fn derive_enum(name: &syn::Ident, generics: &syn:: Generics, data_enum: syn::DataEnum) -> proc_macro::TokenStream {

    if data_enum.variants.iter().all(|variant| variant.fields.is_empty()) {
        return derive_tags(name, generics, data_enum);
    }

    let c_name = format!("{}Container", name);
    let c_ident = syn::Ident::new(&c_name, name.span());

    let r_name = format!("{}Reference", name);
    let r_ident = syn::Ident::new(&r_name, name.span());

    // Record everything we know about the variants.
    // TODO: Distinguish between unit and 0-tuple variants.
    let variants: Vec<(&syn::Ident, Vec<_>)> = 
    data_enum
        .variants
        .iter()
        .map(|variant| (
            &variant.ident, 
            variant.fields.iter().map(|field| &field.ty).collect()
        ))
        .collect();

    // Bit silly, but to help us fit in a byte and reign in bloat.
    assert!(variants.len() <= 256, "Too many variants for enum");

    let names = &variants.iter().map(|(ident, _)| ident).collect::<Vec<_>>();

    // Generic type parameters for the containers for the struct fields.
    let container_types = &names.iter().enumerate().map(|(index, name)| {
        let new_name = format!("C{}", index);
        syn::Ident::new(&new_name, name.span())
    }).collect::<Vec<_>>();
    
    let container_struct = {
        quote! {
            #[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
            #[allow(non_snake_case)]
            pub struct #c_ident < #(#container_types,)* CVar = Vec<u8>, COff = Vec<u64>, >{
                #(pub #names : #container_types, )*
                pub variant: CVar,
                pub offset: COff,
            }
        }
    };

    let reference_struct = {

        let reference_types = &names.iter().enumerate().map(|(index, name)| {
            let new_name = format!("R{}", index);
            syn::Ident::new(&new_name, name.span())
        }).collect::<Vec<_>>();
    
        let ty_gen = quote! { < #(#reference_types),* > };

        quote! {
            #[derive(Copy, Clone, Debug)]
            pub enum #r_ident #ty_gen {
                #(#names(#reference_types),)*
            }
        }
    };

    let push_own = { 

        let (_impl_gen, ty_gen, _where_clause) = generics.split_for_impl();
        
        let push = variants.iter().enumerate().map(|(index, (variant, types))| {

            if types.is_empty() {
                quote! {
                    #name::#variant => {
                        self.offset.push(self.#variant.len() as u64);
                        self.#variant.push(());
                        self.variant.push(#index as u8);
                    }
                }
            }
            else {
                let temp_names = &types.iter().enumerate().map(|(index, _)| {
                    let new_name = format!("t{}", index);
                    syn::Ident::new(&new_name, variant.span())
                }).collect::<Vec<_>>();

                quote! {
                    #name::#variant( #(#temp_names),* ) => {
                        self.offset.push(self.#variant.len() as u64);
                        self.#variant.push((#(#temp_names),*));
                        self.variant.push(#index as u8);
                    },
                }
            }
        });

        let struct_generics = generics.params.iter();
        let impl_gen = quote! { < #(#struct_generics,)* #(#container_types),* > };

        let push_types = variants.iter().map(|(_, types)| quote! { (#(#types),*) });

        let where_clause = quote! { where #(#container_types: ::columnar::Len + ::columnar::Push<#push_types>),* };

        quote! {
            impl #impl_gen ::columnar::Push<#name #ty_gen> for #c_ident < #(#container_types),* > #where_clause {
                fn push(&mut self, item: #name #ty_gen) {
                    match item {
                        #( #push )*
                    }
                }
            }
        }
    };

    let push_ref = { 

        let (_impl_gen, ty_gen, _where_clause) = generics.split_for_impl();
        
        let push = variants.iter().enumerate().map(|(index, (variant, types))| {

            if types.is_empty() {
                quote! {
                    #name::#variant => {
                        self.offset.push(self.#variant.len() as u64);
                        self.#variant.push(());
                        self.variant.push(#index as u8);
                    }
                }
            }
            else {
                let temp_names = &types.iter().enumerate().map(|(index, _)| {
                    let new_name = format!("t{}", index);
                    syn::Ident::new(&new_name, variant.span())
                }).collect::<Vec<_>>();

                quote! {
                    #name::#variant( #(#temp_names),* ) => {
                        self.offset.push(self.#variant.len() as u64);
                        self.#variant.push((#(#temp_names),*));
                        self.variant.push(#index as u8);
                    },
                }
            }
        });

        let struct_generics = generics.params.iter();
        let impl_gen = quote! { < 'columnar, #(#struct_generics,)* #(#container_types),* > };

        let push_types = variants.iter().map(|(_, types)| quote! { (#(&'columnar #types),*) });

        let where_clause = quote! { where #(#container_types: ::columnar::Len + ::columnar::Push<#push_types>),* };

        quote! {
            impl #impl_gen ::columnar::Push<&'columnar #name #ty_gen> for #c_ident < #(#container_types),* > #where_clause {
                fn push(&mut self, item: &'columnar #name #ty_gen) {
                    match item {
                        #( #push )*
                    }
                }
            }
        }
    };

    // Implementation of `Push<#r_ident>`
    let push_new = { 

        let reference_types = &names.iter().enumerate().map(|(index, name)| {
            let new_name = format!("R{}", index);
            syn::Ident::new(&new_name, name.span())
        }).collect::<Vec<_>>();

        let impl_gen = quote! { < #(#container_types,)* #(#reference_types),* > };

        let where_clause = quote! { where #(#container_types: ::columnar::Len + ::columnar::Push<#reference_types>),* };

        let index_type = quote! { #r_ident < #(#reference_types,)* > };
        let numbers = (0 .. variants.len());

        quote! {
            impl #impl_gen ::columnar::Push<#index_type> for #c_ident < #(#container_types),* > #where_clause {
                fn push(&mut self, item: #index_type) {
                    match item {
                        #( 
                            #r_ident::#names(x) => {
                                self.offset.push(self.#names.len() as u64);
                                self.#names.push(x);
                                self.variant.push(#numbers as u8);
                            }, 
                        )*
                    }
                }
            }
        }
    };
    
    let index_own = {
        let impl_gen = quote! { < #(#container_types,)* CVal, COff> };
        let ty_gen = quote! { < #(#container_types,)* CVal, COff> };
        let where_clause = quote! { where #(#container_types: ::columnar::Index,)* CVal: ::columnar::Len + ::columnar::IndexAs<u8>, COff: ::columnar::Len + ::columnar::IndexAs<u64>  };

        let index_type = quote! { #r_ident < #(<#container_types as ::columnar::Index>::Ref,)* > };

        // These numbers must match those in the `Push` implementations.
        let numbers = (0 .. variants.len());

        quote! {
            impl #impl_gen ::columnar::Index for #c_ident #ty_gen #where_clause {
                type Ref = #index_type;
                fn get(&self, index: usize) -> Self::Ref {
                    match self.variant.index_as(index) as usize {
                        #( #numbers => #r_ident::#names(self.#names.get(self.offset.index_as(index) as usize)), )*
                        x => panic!("Unacceptable discriminant found: {:?}", x),
                    }
                }
            }
        }
    };

    let index_ref = {
        let impl_gen = quote! { < 'columnar, #(#container_types,)* CVal, COff> };
        let ty_gen = quote! { < #(#container_types,)* CVal, COff> };
        let where_clause = quote! { where #(&'columnar #container_types: ::columnar::Index,)* CVal: ::columnar::Len + ::columnar::IndexAs<u8>, COff: ::columnar::Len + ::columnar::IndexAs<u64>  };

        let index_type = quote! { #r_ident < #(<&'columnar #container_types as ::columnar::Index>::Ref,)* > };

        // These numbers must match those in the `Push` implementations.
        let numbers = (0 .. variants.len());

        quote! {
            impl #impl_gen ::columnar::Index for &'columnar #c_ident #ty_gen #where_clause {
                type Ref = #index_type;
                fn get(&self, index: usize) -> Self::Ref {
                    match self.variant.index_as(index) as usize {
                        #( #numbers => #r_ident::#names((&self.#names).get(self.offset.index_as(index) as usize)), )*
                        x => panic!("Unacceptable discriminant found: {:?}", x),
                    }
                }
            }
        }
    };

    let clear = { 

        let impl_gen = quote! { < #(#container_types),* > };
        let ty_gen = quote! { < #(#container_types),* > };
        let where_clause = quote! { where #(#container_types: ::columnar::Clear),* };

        quote! {
            impl #impl_gen ::columnar::Clear for #c_ident #ty_gen #where_clause {
                fn clear(&mut self) { 
                    #(self.#names.clear();)* 
                    self.variant.clear();
                    self.offset.clear();
                }
            }
        }
    };

    let length = { 

        let impl_gen = quote! { < #(#container_types,)* CVar, COff> };
        let ty_gen = quote! { < #(#container_types,)* CVar, COff > };

        quote! {
            impl #impl_gen ::columnar::Len for #c_ident #ty_gen where CVar: ::columnar::Len {
                fn len(&self) -> usize {
                    self.variant.len()
                }
            }
        }
    };

    let as_bytes = { 

        let impl_gen = quote! { < #(#container_types,)* CVar, COff> };
        let ty_gen = quote! { < #(#container_types,)* CVar, COff > };
        let where_clause = quote! { where #(#container_types: ::columnar::bytes::AsBytes,)* CVar: ::columnar::bytes::AsBytes, COff: ::columnar::bytes::AsBytes };
        
        quote! {
            impl #impl_gen ::columnar::bytes::AsBytes for #c_ident #ty_gen #where_clause {
                type Borrowed<'columnar> = #c_ident < #(<#container_types as ::columnar::bytes::AsBytes>::Borrowed<'columnar>,)* <CVar as ::columnar::bytes::AsBytes>::Borrowed<'columnar>, <COff as ::columnar::bytes::AsBytes>::Borrowed<'columnar>>;
                fn as_bytes(&self) -> impl Iterator<Item=(u64, &[u8])> {
                    let iter = None.into_iter();
                    #( let iter = iter.chain(self.#names.as_bytes()); )*
                    let iter = iter.chain(self.variant.as_bytes());
                    let iter = iter.chain(self.offset.as_bytes());
                    iter
                }
            }
        }
    };

    let from_bytes = { 

        let impl_gen = quote! { < 'columnar, #(#container_types,)* CVar, COff> };
        let ty_gen = quote! { < #(#container_types,)* CVar, COff > };
        let where_clause = quote! { where #(#container_types: ::columnar::bytes::FromBytes<'columnar>,)* CVar: ::columnar::bytes::FromBytes<'columnar>, COff: ::columnar::bytes::FromBytes<'columnar> };
        
        quote! {
            #[allow(non_snake_case)]
            impl #impl_gen ::columnar::bytes::FromBytes<'columnar> for #c_ident #ty_gen #where_clause {
                fn from_bytes(bytes: &mut impl Iterator<Item=&'columnar [u8]>) -> Self {
                    #(let #names = ::columnar::bytes::FromBytes::from_bytes(bytes);)*
                    let variant = ::columnar::bytes::FromBytes::from_bytes(bytes);
                    let offset = ::columnar::bytes::FromBytes::from_bytes(bytes);
                    Self { #(#names,)* variant, offset }
                }
            }
        }
    };

    let columnar_impl = {

        let (impl_gen, ty_gen, where_clause) = generics.split_for_impl();

        let types = &variants.iter().flat_map(|(_, types)| types).collect::<Vec<_>>();

        let where_clause2 = if let Some(enum_where) = where_clause {
            let params = enum_where.predicates.iter(); 
            quote! {  where #(#types : ::columnar::Columnar,)* #(#params),* }
        }
        else {
            quote! { where #(#types : ::columnar::Columnar,)* }
        };

        let container_types = &variants.iter().map(|(_, types)| quote! { <(#(#types),*) as ::columnar::Columnar>::Container }).collect::<Vec<_>>();

        quote! {
            impl #impl_gen ::columnar::Columnar for #name #ty_gen #where_clause2 {
                type Container = #c_ident < #(#container_types),* >;
            }
        }
    };

    quote! {

        #container_struct
        #reference_struct

        #push_own
        #push_ref
        #push_new

        #index_own
        #index_ref
        #length
        #clear

        #as_bytes
        #from_bytes

        #columnar_impl

    }.into()
}

/// A derivation for an enum type with no fields in any of its variants.
#[allow(unused)]
fn derive_tags(name: &syn::Ident, generics: &syn:: Generics, data_enum: syn::DataEnum) -> proc_macro::TokenStream {

    let c_name = format!("{}Container", name);
    let c_ident = syn::Ident::new(&c_name, name.span());

    let names: Vec<&syn::Ident> =
    data_enum
        .variants
        .iter()
        .map(|variant| &variant.ident)
        .collect();

    let indices: &Vec<u8> = &(0 .. names.len()).map(|x| x as u8).collect();

    // Bit silly, but to help us fit in a byte and reign in bloat.
    assert!(names.len() <= 256, "Too many variants for enum");

    quote! {
        #[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
        pub struct #c_ident <CVar = Vec<u8>> {
            pub variant: CVar,
        }

        impl ::columnar::Push<#name> for #c_ident {
            fn push(&mut self, item: #name) {
                match item {
                    #( #name::#names => self.variant.push(#indices), )*
                }
            }
        }

        impl<'columnar> ::columnar::Push<&'columnar #name> for #c_ident {
            fn push(&mut self, item: &'columnar #name) {
                match *item {
                    #( #name::#names => self.variant.push(#indices), )*
                }
            }
        }

        impl<CVar: ::columnar::Len + ::columnar::IndexAs<u8>> ::columnar::Index for #c_ident <CVar> {
            type Ref = #name;
            fn get(&self, index: usize) -> Self::Ref {
                match self.variant.index_as(index) {
                    #( #indices => #name::#names, )*
                    x => panic!("Unacceptable discriminant found: {:?}", x),
                }
            }
        }

        impl<'columnar, CVar: ::columnar::Len + ::columnar::IndexAs<u8>> ::columnar::Index for &'columnar #c_ident <CVar> {
            type Ref = #name;
            fn get(&self, index: usize) -> Self::Ref {
                match self.variant.index_as(index) {
                    #( #indices => #name::#names, )*
                    x => panic!("Unacceptable discriminant found: {:?}", x),
                }
            }
        }

        impl<CVar: ::columnar::Clear> ::columnar::Clear for #c_ident <CVar> {
            fn clear(&mut self) {
                self.variant.clear();
            }
        }

        impl<CVar: ::columnar::Len> ::columnar::Len for #c_ident <CVar> {
            fn len(&self) -> usize {
                self.variant.len()
            }
        }

        impl<CVar: ::columnar::bytes::AsBytes> ::columnar::bytes::AsBytes for #c_ident <CVar> {
            type Borrowed<'columnar> = #c_ident < <CVar as ::columnar::bytes::AsBytes>::Borrowed<'columnar> >;
            fn as_bytes(&self) -> impl Iterator<Item=(u64, &[u8])> {
                self.variant.as_bytes()
            }
        }

        impl<'columnar, CVar: ::columnar::bytes::FromBytes<'columnar>> ::columnar::bytes::FromBytes<'columnar> for #c_ident <CVar> {
            fn from_bytes(bytes: &mut impl Iterator<Item=&'columnar [u8]>) -> Self {
                Self { variant: ::columnar::bytes::FromBytes::from_bytes(bytes) }
            }
        }

        impl ::columnar::Columnar for #name {
            type Container = #c_ident;
        }
    }.into()
}
