extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Attribute, DeriveInput};

#[proc_macro_derive(Columnar, attributes(columnar))]
pub fn derive(input: TokenStream) -> TokenStream {

    let ast = parse_macro_input!(input as DeriveInput);
    let name = &ast.ident;

    let attr = extract_attr(&ast.attrs);

    match ast.data {
        syn::Data::Struct(data_struct) => {
            match data_struct.fields {
                syn::Fields::Unit => derive_unit_struct(name, &ast.generics, ast.vis, attr),
                _ => derive_struct(name, &ast.generics, data_struct, ast.vis, attr),
            }
        }
        syn::Data::Enum(data_enum) => {
            derive_enum(name, &ast.generics, data_enum, ast.vis, attr)
        }
        syn::Data::Union(_) => unimplemented!("Unions are unsupported by Columnar"),
    }
}

fn extract_attr(attrs: &[Attribute]) -> Option<proc_macro2::TokenStream> {
    for attr in attrs {
        if attr.path().is_ident("columnar") {
            return Some(attr.parse_args().unwrap());
        }
    }
    None
}

fn derive_struct(name: &syn::Ident, generics: &syn::Generics, data_struct: syn::DataStruct, vis: syn::Visibility, attr: Option<proc_macro2::TokenStream>) -> proc_macro::TokenStream {

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
    #[cfg(feature = "serde")]
    let derive = quote! { #[derive(Copy, Clone, Debug, Default, serde::Serialize, serde::Deserialize)] };
    #[cfg(not(feature = "serde"))]
    let derive = quote! { #[derive(Copy, Clone, Debug, Default)] };

    let container_struct = {
        let field_docs: Vec<String> = names.iter().map(|n| format!("Container for `{}`.", n)).collect();
        quote! {
            /// Derived columnar container for a struct.
            #derive
            #vis struct #c_ident < #(#container_types),* >{
                #(
                    #[doc = #field_docs]
                    pub #names : #container_types,
                )*
            }
        }
    };

    let reference_struct = {

        let reference_types = &names.iter().enumerate().map(|(index, name)| {
            let new_name = format!("R{}", index);
            syn::Ident::new(&new_name, name.span())
        }).collect::<Vec<_>>();

        let ty_gen = quote! { < #(#reference_types),* > };

        let attr = if let Some(attr) = attr {
            quote! { #[#attr] }
        } else {
            quote! {}
        };

        let field_docs: Vec<String> = names.iter().map(|n| format!("Field for `{}`.", n)).collect();
        quote! {
            /// Derived columnar reference for a struct.
            #[derive(Copy, Clone, Debug)]
            #attr
            #vis struct #r_ident #ty_gen {
                #(
                    #[doc = #field_docs]
                    pub #names : #reference_types,
                )*
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
                #[inline(always)]
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
                #[inline]
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
                #[inline]
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
                #[inline]
                fn push(&mut self, item: #index_type) {
                    #destructure_self
                    #(#push)*
                }
            }
        }
    };

    let seq_iter_struct = {
        let seq_iter_ident = syn::Ident::new(&format!("{}SeqIter", c_ident), c_ident.span());
        let first_name = &names[0];
        quote! {
            /// Composed SeqIter for the derived container: zips field `Sequence::Iter`s.
            #vis struct #seq_iter_ident < #(#container_types),* > {
                #( pub #names: #container_types, )*
            }

            impl< #(#container_types: Iterator),* > Iterator for #seq_iter_ident < #(#container_types),* > {
                type Item = #r_ident < #(#container_types::Item,)* >;
                #[inline(always)]
                fn next(&mut self) -> Option<Self::Item> {
                    Some(#r_ident { #( #names: self.#names.next()?, )* })
                }
                #[inline(always)]
                fn size_hint(&self) -> (usize, Option<usize>) {
                    self.#first_name.size_hint()
                }
            }

            impl< #(#container_types: ExactSizeIterator),* > ExactSizeIterator for #seq_iter_ident < #(#container_types),* > {}
        }
    };

    let sequence_impl = {
        let seq_iter_ident = syn::Ident::new(&format!("{}SeqIter", c_ident), c_ident.span());
        let impl_gen = quote! { < #(#container_types: ::columnar::Sequence),* > };
        let ty_gen = quote! { < #(#container_types),* > };

        quote! {
            impl #impl_gen ::columnar::Sequence for #c_ident #ty_gen
            where
                #c_ident #ty_gen : Copy + ::columnar::Len,
            {
                type Ref = #r_ident < #(<#container_types as ::columnar::Sequence>::Ref,)* >;
                type Iter = #seq_iter_ident < #(<#container_types as ::columnar::Sequence>::Iter,)* >;
                #[inline(always)]
                fn seq_iter(self) -> Self::Iter {
                    #seq_iter_ident { #( #names: <#container_types as ::columnar::Sequence>::seq_iter(self.#names), )* }
                }
                #[inline(always)]
                fn seq_iter_range(self, range: ::core::ops::Range<usize>) -> Self::Iter {
                    #seq_iter_ident { #( #names: <#container_types as ::columnar::Sequence>::seq_iter_range(self.#names, range.clone()), )* }
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
                #[inline(always)]
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
                #[inline(always)]
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
                #[inline(always)]
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
                #[inline(always)]
                fn len(&self) -> usize {
                    self.#first_name.len()
                }
            }
        }
    };

    let as_bytes = {

        let impl_gen = quote! { <'a, #(#container_types),* > };
        let ty_gen = quote! { < #(#container_types),* > };
        let where_clause = quote! { where #(#container_types: ::columnar::AsBytes<'a>),* };

        quote! {
            impl #impl_gen ::columnar::AsBytes<'a> for #c_ident #ty_gen #where_clause {
                const SLICE_COUNT: usize = 0 #(+ <#container_types as ::columnar::AsBytes<'a>>::SLICE_COUNT)*;
                #[inline]
                fn get_byte_slice(&self, index: usize) -> (u64, &'a [u8]) {
                    debug_assert!(index < Self::SLICE_COUNT);
                    let mut _offset = 0;
                    #(
                        if index < _offset + <#container_types as ::columnar::AsBytes<'a>>::SLICE_COUNT {
                            return self.#names.get_byte_slice(index - _offset);
                        }
                        _offset += <#container_types as ::columnar::AsBytes<'a>>::SLICE_COUNT;
                    )*
                    panic!("get_byte_slice: index out of bounds")
                }
            }
        }
    };

    let from_bytes = {

        let impl_gen = quote! { < 'columnar, #(#container_types),* > };
        let ty_gen = quote! { < #(#container_types),* > };
        let where_clause = quote! { where #(#container_types: ::columnar::FromBytes<'columnar>),* };

        quote! {
            impl #impl_gen ::columnar::FromBytes<'columnar> for #c_ident #ty_gen #where_clause {
                const SLICE_COUNT: usize = 0 #(+ <#container_types>::SLICE_COUNT)*;
                #[inline(always)]
                fn from_bytes(bytes: &mut impl Iterator<Item=&'columnar [u8]>) -> Self {
                    Self { #(#names: ::columnar::FromBytes::from_bytes(bytes),)* }
                }
                #[inline(always)]
                fn from_store(store: &::columnar::bytes::indexed::DecodedStore<'columnar>, offset: &mut usize) -> Self {
                    Self { #(#names: ::columnar::FromBytes::from_store(store, offset),)* }
                }
                fn element_sizes(sizes: &mut Vec<usize>) -> ::core::result::Result<(), String> {
                    #(<#container_types>::element_sizes(sizes)?;)*
                    Ok(())
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

        // Either use curly braces or parentheses to destructure the item.
        let destructure_self =
        if named { quote! { let #name { #(#names),* } = self; } }
        else     { quote! { let #name ( #(#names),* ) = self; } };

        // Either use curly braces or parentheses to destructure the item.
        let into_self =
        if named { quote! { #name { #(#names: ::columnar::Columnar::into_owned(other.#names)),* } } }
        else     { quote! { #name ( #(::columnar::Columnar::into_owned(other.#names)),* ) } };

        quote! {
            impl #impl_gen ::columnar::Columnar for #name #ty_gen #where_clause2 {
                #[inline(always)]
                fn copy_from<'a>(&mut self, other: ::columnar::Ref<'a, Self>) {
                    #destructure_self
                    #( ::columnar::Columnar::copy_from(#names, other.#names); )*
                }
                #[inline(always)]
                fn into_owned<'a>(other: ::columnar::Ref<'a, Self>) -> Self {
                    #into_self
                }
                type Container = #c_ident < #(<#types as ::columnar::Columnar>::Container ),* >;
            }

            impl < #( #container_types: ::columnar::Borrow ),* > ::columnar::Borrow for #c_ident < #( #container_types ),* > {
                type Ref<'a> = #r_ident < #(<#container_types as ::columnar::Borrow>::Ref<'a>,)* > where #(#container_types: 'a,)*;
                type Borrowed<'a> = #c_ident < #(<#container_types as ::columnar::Borrow>::Borrowed<'a> ),* > where #(#container_types: 'a,)*;
                #[inline(always)]
                fn borrow<'a>(&'a self) -> Self::Borrowed<'a> {
                    #c_ident {
                        #( #names: <#container_types as ::columnar::Borrow>::borrow(&self.#names), )*
                    }
                }
                #[inline(always)]
                fn reborrow<'b, 'a: 'b>(thing: Self::Borrowed<'a>) -> Self::Borrowed<'b> {
                    #c_ident {
                        #( #names: <#container_types as ::columnar::Borrow>::reborrow(thing.#names), )*
                    }
                }
                #[inline(always)]
                fn reborrow_ref<'b, 'a: 'b>(thing: Self::Ref<'a>) -> Self::Ref<'b> {
                    #r_ident {
                        #( #names: <#container_types as ::columnar::Borrow>::reborrow_ref(thing.#names), )*
                    }
                }
            }

            impl < #( #container_types: ::columnar::Container ),* > ::columnar::Container for #c_ident < #( #container_types ),* > {
                #[inline(always)]
                fn extend_from_self(&mut self, other: Self::Borrowed<'_>, range: std::ops::Range<usize>) {
                    #( self.#names.extend_from_self(other.#names, range.clone()); )*
                }

                fn reserve_for<'a, I>(&mut self, selves: I) where Self: 'a, I: Iterator<Item = Self::Borrowed<'a>> + Clone {
                    #( self.#names.reserve_for(selves.clone().map(|x| x.#names)); )*
                }
            }
        }
    };


    quote! {

        #container_struct
        #reference_struct
        #seq_iter_struct

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

        #sequence_impl

    }.into()
}

// TODO: Do we need to use the generics?
fn derive_unit_struct(name: &syn::Ident, _generics: &syn::Generics, vis: syn::Visibility, attr: Option<proc_macro2::TokenStream>) -> proc_macro::TokenStream {

    let c_name = format!("{}Container", name);
    let c_ident = syn::Ident::new(&c_name, name.span());

    if attr.is_some() {
        panic!("Unit structs do not support attributes");
    }

    #[cfg(feature = "serde")]
    let derive = quote! { #[derive(Copy, Clone, Debug, Default, serde::Serialize, serde::Deserialize)] };
    #[cfg(not(feature = "serde"))]
    let derive = quote! { #[derive(Copy, Clone, Debug, Default)] };

    quote! {

        /// Derived columnar container for a unit struct.
        #derive
        #vis struct #c_ident<CW = u64> {
            /// Count of the number of contained records.
            pub count: CW,
        }

        impl ::columnar::Push<#name> for #c_ident {
            #[inline]
            fn push(&mut self, _item: #name) {
                self.count += 1;
            }
        }

        impl<'columnar> ::columnar::Push<&'columnar #name> for #c_ident {
            #[inline]
            fn push(&mut self, _item: &'columnar #name) {
                self.count += 1;
            }
        }

        impl<CW> ::columnar::Index for #c_ident<CW> {
            type Ref = #name;
            #[inline(always)]
            fn get(&self, index: usize) -> Self::Ref {
                #name
            }
        }

        impl<'columnar, CW> ::columnar::Index for &'columnar #c_ident<CW> {
            type Ref = #name;
            #[inline(always)]
            fn get(&self, index: usize) -> Self::Ref {
                #name
            }
        }

        impl ::columnar::Clear for #c_ident {
            #[inline(always)]
            fn clear(&mut self) {
                self.count = 0;
            }
        }

        impl<CW: Copy+::columnar::common::index::CopyAs<u64>> ::columnar::Len for #c_ident<CW> {
            #[inline(always)]
            fn len(&self) -> usize {
                use ::columnar::common::index::CopyAs;
                self.count.copy_as() as usize
            }
        }

        impl<'a> ::columnar::AsBytes<'a> for #c_ident <&'a u64> {
            const SLICE_COUNT: usize = 1;
            #[inline]
            fn get_byte_slice(&self, index: usize) -> (u64, &'a [u8]) {
                debug_assert!(index < Self::SLICE_COUNT);
                (8, ::columnar::bytemuck::cast_slice(::core::slice::from_ref(self.count)))
            }
        }

        impl<'columnar> ::columnar::FromBytes<'columnar> for #c_ident <&'columnar u64> {
            const SLICE_COUNT: usize = 1;
            #[inline(always)]
            fn from_bytes(bytes: &mut impl Iterator<Item=&'columnar [u8]>) -> Self {
                Self { count: &::columnar::bytemuck::try_cast_slice(bytes.next().unwrap()).unwrap()[0] }
            }
            #[inline(always)]
            fn from_store(store: &::columnar::bytes::indexed::DecodedStore<'columnar>, offset: &mut usize) -> Self {
                let (w, _tail) = store.get(*offset);
                *offset += 1;
                Self { count: w.first().unwrap_or(&0) }
            }
            fn element_sizes(sizes: &mut Vec<usize>) -> ::core::result::Result<(), String> {
                sizes.push(8);
                Ok(())
            }
        }

        impl ::columnar::Columnar for #name {
            #[inline(always)]
            fn copy_from<'a>(&mut self, other: ::columnar::Ref<'a, Self>) { *self = other; }
            #[inline(always)]
            fn into_owned<'a>(other: ::columnar::Ref<'a, Self>) -> Self { other }
            type Container = #c_ident;
        }

        impl ::columnar::Borrow for #c_ident {
            type Ref<'a> = #name;
            type Borrowed<'a> = #c_ident < &'a u64 >;
            #[inline(always)]
            fn borrow<'a>(&'a self) -> Self::Borrowed<'a> {
                #c_ident { count: &self.count }
            }
            #[inline(always)]
            fn reborrow<'b, 'a: 'b>(thing: Self::Borrowed<'a>) -> Self::Borrowed<'b> {
                #c_ident { count: thing.count }
            }
            #[inline(always)]
            fn reborrow_ref<'b, 'a: 'b>(thing: Self::Ref<'a>) -> Self::Ref<'b> { thing }
        }

        impl ::columnar::Container for #c_ident {
            #[inline(always)]
            fn extend_from_self(&mut self, _other: Self::Borrowed<'_>, range: std::ops::Range<usize>) {
                self.count += range.len() as u64;
            }

            fn reserve_for<'a, I>(&mut self, selves: I) where Self: 'a, I: Iterator<Item = Self::Borrowed<'a>> + Clone { }
        }

    }.into()
}

/// The derived container for an `enum` type will be a struct with containers for each field of each variant, plus an offset container and a discriminant container.
/// Its index `Ref` type will be an enum with parallel variants, each containing the index `Ref` types of the corresponding variant containers.
#[allow(unused)]
fn derive_enum(name: &syn::Ident, generics: &syn:: Generics, data_enum: syn::DataEnum, vis: syn::Visibility, attr: Option<proc_macro2::TokenStream>) -> proc_macro::TokenStream {

    if data_enum.variants.iter().all(|variant| variant.fields.is_empty()) {
        return derive_tags(name, generics, data_enum, vis);
    }

    let c_name = format!("{}Container", name);
    let c_ident = syn::Ident::new(&c_name, name.span());

    let r_name = format!("{}Reference", name);
    let r_ident = syn::Ident::new(&r_name, name.span());

    // Record everything we know about the variants.
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

    #[cfg(feature = "serde")]
    let derive = quote! { #[derive(Copy, Clone, Debug, Default, serde::Serialize, serde::Deserialize)] };
    #[cfg(not(feature = "serde"))]
    let derive = quote! { #[derive(Copy, Clone, Debug, Default)] };

    let container_struct = {
        quote! {
            /// Derived columnar container for an enum.
            #derive
            #[allow(non_snake_case)]
            #vis struct #c_ident < #(#container_types,)* CVar = Vec<u8>, COff = Vec<u64>, >{
                #(
                    /// Container for #names.
                    pub #names : #container_types,
                )*
                /// Discriminant tracking for variants.
                pub indexes: ::columnar::Discriminant<CVar, COff>,
            }
        }
    };

    let reference_struct = {

        let reference_types = &names.iter().enumerate().map(|(index, name)| {
            let new_name = format!("R{}", index);
            syn::Ident::new(&new_name, name.span())
        }).collect::<Vec<_>>();

        let ty_gen = quote! { < #(#reference_types),* > };

        let attr = if let Some(attr) = attr {
            quote! { #[#attr] }
        } else {
            quote! {}
        };


        quote! {
            /// Reference for an enum.
            #[derive(Copy, Clone, Debug)]
            #attr
            #vis enum #r_ident #ty_gen {
                #(
                    /// Enum variant for #names.
                    #names(#reference_types),
                )*
            }
        }
    };

    let push_own = {

        let (_impl_gen, ty_gen, _where_clause) = generics.split_for_impl();

        let push = variants.iter().enumerate().map(|(index, (variant, types))| {

            match &data_enum.variants[index].fields {
                syn::Fields::Unit => {
                    quote! {
                        #name::#variant => {
                            self.indexes.push(#index as u8, self.#variant.len() as u64);
                            self.#variant.push(());
                        }
                    }
                }
                syn::Fields::Unnamed(_) => {
                    let temp_names = &types.iter().enumerate().map(|(index, _)| {
                        let new_name = format!("t{}", index);
                        syn::Ident::new(&new_name, variant.span())
                    }).collect::<Vec<_>>();

                    quote! {
                        #name::#variant( #(#temp_names),* ) => {
                            self.indexes.push(#index as u8, self.#variant.len() as u64);
                            self.#variant.push((#(#temp_names),*));
                        },
                    }
                }
                syn::Fields::Named(fields) => {
                    let field_names = &fields.named.iter().map(|f| f.ident.as_ref().unwrap()).collect::<Vec<_>>();

                    quote! {
                        #name::#variant { #(#field_names),* } => {
                            self.indexes.push(#index as u8, self.#variant.len() as u64);
                            self.#variant.push((#(#field_names),*));
                        },
                    }
                }
            }
        });

        let struct_generics = generics.params.iter();
        let impl_gen = quote! { < #(#struct_generics,)* #(#container_types),* > };

        let push_types = variants.iter().map(|(_, types)| quote! { (#(#types),*) });

        let where_clause = quote! { where #(#container_types: ::columnar::Len + ::columnar::Push<#push_types>),* };

        quote! {
            impl #impl_gen ::columnar::Push<#name #ty_gen> for #c_ident < #(#container_types),* > #where_clause {
                #[inline]
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

            match &data_enum.variants[index].fields {
                syn::Fields::Unit => {
                    quote! {
                        #name::#variant => {
                            self.indexes.push(#index as u8, self.#variant.len() as u64);
                            self.#variant.push(());
                        }
                    }
                }
                syn::Fields::Unnamed(_) => {
                    let temp_names = &types.iter().enumerate().map(|(index, _)| {
                        let new_name = format!("t{}", index);
                        syn::Ident::new(&new_name, variant.span())
                    }).collect::<Vec<_>>();

                    quote! {
                        #name::#variant( #(#temp_names),* ) => {
                            self.indexes.push(#index as u8, self.#variant.len() as u64);
                            self.#variant.push((#(#temp_names),*));
                        },
                    }
                }
                syn::Fields::Named(fields) => {
                    let field_names = &fields.named.iter().map(|f| f.ident.as_ref().unwrap()).collect::<Vec<_>>();

                    quote! {
                        #name::#variant { #(#field_names),* } => {
                            self.indexes.push(#index as u8, self.#variant.len() as u64);
                            self.#variant.push((#(#field_names),*));
                        },
                    }
                }
            }
        });

        let struct_generics = generics.params.iter();
        let impl_gen = quote! { < 'columnar, #(#struct_generics,)* #(#container_types),* > };

        let push_types = variants.iter().map(|(_, types)| quote! { (#(&'columnar #types),*) });

        let where_clause = quote! { where #(#container_types: ::columnar::Len + ::columnar::Push<#push_types>),* };

        quote! {
            impl #impl_gen ::columnar::Push<&'columnar #name #ty_gen> for #c_ident < #(#container_types),* > #where_clause {
                #[inline]
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
                #[inline]
                fn push(&mut self, item: #index_type) {
                    match item {
                        #(
                            #r_ident::#names(x) => {
                                self.indexes.push(#numbers as u8, self.#names.len() as u64);
                                self.#names.push(x);
                            },
                        )*
                    }
                }
            }
        }
    };

    let index_own = {
        let impl_gen = quote! { < #(#container_types,)* CVar, COff> };
        let ty_gen = quote! { < #(#container_types,)* CVar, COff> };
        let where_clause = quote! { where #(#container_types: ::columnar::Index,)* CVar: ::columnar::Len + ::columnar::IndexAs<u8>, COff: ::columnar::Len + ::columnar::IndexAs<u64>  };

        let index_type = quote! { #r_ident < #(<#container_types as ::columnar::Index>::Ref,)* > };

        // These numbers must match those in the `Push` implementations.
        let numbers = (0 .. variants.len());

        quote! {
            impl #impl_gen ::columnar::Index for #c_ident #ty_gen #where_clause {
                type Ref = #index_type;
                #[inline(always)]
                fn get(&self, index: usize) -> Self::Ref {
                    let (variant, offset) = self.indexes.get(index);
                    match variant as usize {
                        #( #numbers => #r_ident::#names(self.#names.get(offset as usize)), )*
                        x => panic!("Unacceptable discriminant found: {:?}", x),
                    }
                }
            }
        }
    };

    let index_ref = {
        let impl_gen = quote! { < 'columnar, #(#container_types,)* CVar, COff> };
        let ty_gen = quote! { < #(#container_types,)* CVar, COff> };
        let where_clause = quote! { where #(&'columnar #container_types: ::columnar::Index,)* CVar: ::columnar::Len + ::columnar::IndexAs<u8>, COff: ::columnar::Len + ::columnar::IndexAs<u64>  };

        let index_type = quote! { #r_ident < #(<&'columnar #container_types as ::columnar::Index>::Ref,)* > };

        // These numbers must match those in the `Push` implementations.
        let numbers = (0 .. variants.len());

        quote! {
            impl #impl_gen ::columnar::Index for &'columnar #c_ident #ty_gen #where_clause {
                type Ref = #index_type;
                #[inline(always)]
                fn get(&self, index: usize) -> Self::Ref {
                    let (variant, offset) = self.indexes.get(index);
                    match variant as usize {
                        #( #numbers => #r_ident::#names((&self.#names).get(offset as usize)), )*
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
                #[inline(always)]
                fn clear(&mut self) {
                    #(self.#names.clear();)*
                    self.indexes.clear();
                }
            }
        }
    };

    let length = {

        let impl_gen = quote! { < #(#container_types,)* CVar, COff> };
        let ty_gen = quote! { < #(#container_types,)* CVar, COff > };

        quote! {
            impl #impl_gen ::columnar::Len for #c_ident #ty_gen where CVar: ::columnar::Len, COff: ::columnar::Len + ::columnar::IndexAs<u64> {
                #[inline(always)]
                fn len(&self) -> usize {
                    self.indexes.len()
                }
            }
        }
    };

    let as_bytes = {

        let impl_gen = quote! { < 'a, #(#container_types,)* CVar, COff> };
        let ty_gen = quote! { < #(#container_types,)* CVar, COff > };
        let where_clause = quote! { where #(#container_types: ::columnar::AsBytes<'a>,)* ::columnar::Discriminant<CVar, COff>: ::columnar::AsBytes<'a> };

        quote! {
            impl #impl_gen ::columnar::AsBytes<'a> for #c_ident #ty_gen #where_clause {
                const SLICE_COUNT: usize = 0 #(+ <#container_types as ::columnar::AsBytes<'a>>::SLICE_COUNT)* + <::columnar::Discriminant<CVar, COff> as ::columnar::AsBytes<'a>>::SLICE_COUNT;
                #[inline]
                fn get_byte_slice(&self, index: usize) -> (u64, &'a [u8]) {
                    debug_assert!(index < Self::SLICE_COUNT);
                    let mut _offset = 0;
                    #(
                        if index < _offset + <#container_types as ::columnar::AsBytes<'a>>::SLICE_COUNT {
                            return self.#names.get_byte_slice(index - _offset);
                        }
                        _offset += <#container_types as ::columnar::AsBytes<'a>>::SLICE_COUNT;
                    )*
                    self.indexes.get_byte_slice(index - _offset)
                }
            }
        }
    };

    let from_bytes = {

        let impl_gen = quote! { < 'columnar, #(#container_types,)* CVar, COff> };
        let ty_gen = quote! { < #(#container_types,)* CVar, COff > };
        let where_clause = quote! { where #(#container_types: ::columnar::FromBytes<'columnar>,)* ::columnar::Discriminant<CVar, COff>: ::columnar::FromBytes<'columnar> };

        quote! {
            #[allow(non_snake_case)]
            impl #impl_gen ::columnar::FromBytes<'columnar> for #c_ident #ty_gen #where_clause {
                const SLICE_COUNT: usize = 0 #(+ <#container_types>::SLICE_COUNT)* + <::columnar::Discriminant<CVar, COff>>::SLICE_COUNT;
                #[inline(always)]
                fn from_bytes(bytes: &mut impl Iterator<Item=&'columnar [u8]>) -> Self {
                    Self {
                        #(#names: ::columnar::FromBytes::from_bytes(bytes),)*
                        indexes: ::columnar::FromBytes::from_bytes(bytes),
                    }
                }
                #[inline(always)]
                fn from_store(store: &::columnar::bytes::indexed::DecodedStore<'columnar>, offset: &mut usize) -> Self {
                    Self {
                        #(#names: ::columnar::FromBytes::from_store(store, offset),)*
                        indexes: ::columnar::FromBytes::from_store(store, offset),
                    }
                }
                fn element_sizes(sizes: &mut Vec<usize>) -> ::core::result::Result<(), String> {
                    #(<#container_types>::element_sizes(sizes)?;)*
                    <::columnar::Discriminant<CVar, COff>>::element_sizes(sizes)?;
                    Ok(())
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


        let variant_types = &variants.iter().map(|(_, types)| quote! { (#(#types),*) }).collect::<Vec<_>>();

        let container_types = &variants.iter().map(|(_, types)| quote! { <(#(#types),*) as ::columnar::Columnar>::Container }).collect::<Vec<_>>();
        // Generic type parameters for the containers for the struct fields.
        let container_names = &names.iter().enumerate().map(|(index, name)| {
            let new_name = format!("C{}", index);
            syn::Ident::new(&new_name, name.span())
        }).collect::<Vec<_>>();

        let reference_args = variants.iter().map(|(_, types)| quote! { <(#(#types),*) as ::columnar::Columnar>::Ref<'a> });
        let reference_args2 = reference_args.clone();

        // For each variant of `other`, the matching and non-matching variant cases.
        let copy_from = variants.iter().enumerate().map(|(index, (variant, types))| {

            if data_enum.variants[index].fields == syn::Fields::Unit {
                quote! {
                    (#name::#variant, #r_ident::#variant(_)) => { }
                    (_, #r_ident::#variant(_)) => { *self = #name::#variant; }
                }
            }
            else {
                let temp_names1 = &types.iter().enumerate().map(|(index, _)| {
                    let new_name = format!("s{}", index);
                    syn::Ident::new(&new_name, variant.span())
                }).collect::<Vec<_>>();
                let temp_names2 = &types.iter().enumerate().map(|(index, _)| {
                    let new_name = format!("t{}", index);
                    syn::Ident::new(&new_name, variant.span())
                }).collect::<Vec<_>>();

                let destructure = match &data_enum.variants[index].fields {
                    syn::Fields::Named(fields) => {
                        let field_names: Vec<_> = fields.named.iter().map(|f| f.ident.as_ref().unwrap()).collect();
                        quote! { #name::#variant { #(#field_names: #temp_names1),* } }
                    }
                    _ => quote! { #name::#variant( #(#temp_names1),* ) }
                };

                quote! {
                    (#destructure, #r_ident::#variant( ( #( #temp_names2 ),* ) )) => {
                        #( ::columnar::Columnar::copy_from(#temp_names1, #temp_names2); )*
                    }
                }
            }
        }).collect::<Vec<_>>();

        // For each variant of `other`, the matching and non-matching variant cases.
        let into_owned = variants.iter().enumerate().map(|(index, (variant, types))| {

            if data_enum.variants[index].fields == syn::Fields::Unit {
                quote! { #r_ident::#variant(_) => #name::#variant, }
            }
            else {
                let temp_names = &types.iter().enumerate().map(|(index, _)| {
                    let new_name = format!("t{}", index);
                    syn::Ident::new(&new_name, variant.span())
                }).collect::<Vec<_>>();

                let reconstruct = match &data_enum.variants[index].fields {
                    syn::Fields::Named(fields) => {
                        let field_names: Vec<_> = fields.named.iter().map(|f| f.ident.as_ref().unwrap()).collect();
                        quote! { #name::#variant { #(#field_names: ::columnar::Columnar::into_owned(#temp_names)),* } }
                    }
                    _ => quote! { #name::#variant( #( ::columnar::Columnar::into_owned(#temp_names) ),* ) }
                };

                quote! {
                    #r_ident::#variant(( #( #temp_names ),* )) => {
                        #reconstruct
                    },
                }
            }
        }).collect::<Vec<_>>();

        // For each variant, the reborrow case.
        let reborrow_ref = variants.iter().enumerate().zip(container_names.iter()).map(|((index, (variant, types)), cname)| {
            quote! {
                #r_ident::#variant(( potato )) => {
                    #r_ident::#variant((  < (#cname) as ::columnar::Borrow >::reborrow_ref::<'b, 'a>( potato ) ))
                },
            }
        }).collect::<Vec<_>>();

        // Helper identifiers for `extend_from_self` local variables.
        let len_idents = &names.iter().map(|n| syn::Ident::new(&format!("len_{}", n.to_string().to_lowercase()), n.span())).collect::<Vec<_>>();
        let count_idents = &names.iter().map(|n| syn::Ident::new(&format!("count_{}", n.to_string().to_lowercase()), n.span())).collect::<Vec<_>>();
        let start_idents = &names.iter().map(|n| syn::Ident::new(&format!("start_{}", n.to_string().to_lowercase()), n.span())).collect::<Vec<_>>();
        let variant_indices = &(0..variants.len()).map(|i| i as u8).collect::<Vec<_>>();

        quote! {
            impl #impl_gen ::columnar::Columnar for #name #ty_gen #where_clause2 {
                #[inline(always)]
                fn copy_from<'a>(&mut self, other: ::columnar::Ref<'a, Self>) {
                    match (&mut *self, other) {
                        #( #copy_from )*
                        (_, other) => { *self = Self::into_owned(other); }
                    }
                }
                #[inline(always)]
                fn into_owned<'a>(other: ::columnar::Ref<'a, Self>) -> Self {
                    match other {
                        #( #into_owned )*
                    }
                }
                type Container = #c_ident < #(#container_types),* >;
            }

            impl < #(#container_names : ::columnar::Borrow ),* > ::columnar::Borrow for #c_ident < #(#container_names),* > {
                type Ref<'a> = #r_ident < #( <#container_names as ::columnar::Borrow>::Ref<'a> ,)* > where Self: 'a, #(#container_names: 'a,)*;
                type Borrowed<'a> = #c_ident < #( < #container_names as ::columnar::Borrow >::Borrowed<'a>, )* &'a [u8], &'a [u64] > where #(#container_names: 'a,)*;
                #[inline(always)]
                fn borrow<'a>(&'a self) -> Self::Borrowed<'a> {
                    #c_ident {
                        #(#names: self.#names.borrow(),)*
                        indexes: self.indexes.borrow(),
                    }
                }
                #[inline(always)]
                fn reborrow<'b, 'a: 'b>(thing: Self::Borrowed<'a>) -> Self::Borrowed<'b> {
                    #c_ident {
                        #(#names: <#container_names as ::columnar::Borrow>::reborrow(thing.#names),)*
                        indexes: <::columnar::Discriminant as ::columnar::Borrow>::reborrow(thing.indexes),
                    }
                }
                #[inline(always)]
                fn reborrow_ref<'b, 'a: 'b>(thing: Self::Ref<'a>) -> Self::Ref<'b> {
                    match thing {
                        #( #reborrow_ref )*
                    }
                }
            }

            impl < #(#container_names : ::columnar::Container + ::columnar::Len),* > ::columnar::Container for #c_ident < #(#container_names),* > {
                #[inline(always)]
                fn extend_from_self(&mut self, other: Self::Borrowed<'_>, range: std::ops::Range<usize>) {
                    if !range.is_empty() {
                        #( let #len_idents = ::columnar::Len::len(&self.#names); )*
                        #( let mut #count_idents = 0usize; )*
                        #( let mut #start_idents = 0u64; )*
                        for index in range.clone() {
                            let (variant, offset) = other.indexes.get(index);
                            match variant {
                                #(
                                    #variant_indices => {
                                        if #count_idents == 0 { #start_idents = offset; }
                                        self.indexes.push(#variant_indices, (#len_idents + #count_idents) as u64);
                                        #count_idents += 1;
                                    }
                                )*
                                _ => unreachable!(),
                            }
                        }
                        #(
                            if #count_idents > 0 {
                                self.#names.extend_from_self(other.#names, #start_idents as usize .. #start_idents as usize + #count_idents);
                            }
                        )*
                    }
                }

                fn reserve_for<'a, I>(&mut self, selves: I) where Self: 'a, I: Iterator<Item = Self::Borrowed<'a>> + Clone {
                    #( self.#names.reserve_for(selves.clone().map(|x| x.#names)); )*
                    self.indexes.reserve_for(selves.map(|x| x.indexes));
                }
            }
        }
    };

    let try_unwrap = {
        let impl_gen = quote! { < #(#container_types),* > };
        let ty_gen = quote! { < #(#container_types),* > };

        let numbers = (0u8 ..);
        let methods = names.iter().zip(container_types.iter()).zip(numbers).map(|((name, ctype), index)| {
            let try_name = syn::Ident::new(&format!("try_unwrap_{}", name), name.span());
            quote! {
                /// Returns the #name container if all elements are #name.
                #[inline]
                pub fn #try_name(&self) -> Option<&#ctype> {
                    if self.indexes.homogeneous() == Some(#index) { Some(&self.#name) } else { None }
                }
            }
        });

        quote! {
            #[allow(non_snake_case)]
            impl #impl_gen #c_ident #ty_gen {
                #( #methods )*
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

        #try_unwrap

    }.into()
}

/// A derivation for an enum type with no fields in any of its variants.
#[allow(unused)]
fn derive_tags(name: &syn::Ident, _generics: &syn:: Generics, data_enum: syn::DataEnum, vis: syn::Visibility) -> proc_macro::TokenStream {

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

    #[cfg(feature = "serde")]
    let derive = quote! { #[derive(Copy, Clone, Debug, Default, serde::Serialize, serde::Deserialize)] };
    #[cfg(not(feature = "serde"))]
    let derive = quote! { #[derive(Copy, Clone, Debug, Default)] };

    quote! {
        /// Derived columnar container for all-unit enum.
        #derive
        #vis struct #c_ident <CVar = Vec<u8>> {
            /// Container for variant.
            pub variant: CVar,
        }

        impl<CV: ::columnar::common::PushIndexAs<u8>> ::columnar::Push<#name> for #c_ident<CV> {
            #[inline]
            fn push(&mut self, item: #name) {
                match item {
                    #( #name::#names => self.variant.push(&#indices), )*
                }
            }
        }

        impl<'columnar> ::columnar::Push<&'columnar #name> for #c_ident {
            #[inline]
            fn push(&mut self, item: &'columnar #name) {
                match *item {
                    #( #name::#names => self.variant.push(#indices), )*
                }
            }
        }

        impl<CVar: ::columnar::Len + ::columnar::IndexAs<u8>> ::columnar::Index for #c_ident <CVar> {
            type Ref = #name;
            #[inline(always)]
            fn get(&self, index: usize) -> Self::Ref {
                match self.variant.index_as(index) {
                    #( #indices => #name::#names, )*
                    x => panic!("Unacceptable discriminant found: {:?}", x),
                }
            }
        }

        impl<'columnar, CVar: ::columnar::Len + ::columnar::IndexAs<u8>> ::columnar::Index for &'columnar #c_ident <CVar> {
            type Ref = #name;
            #[inline(always)]
            fn get(&self, index: usize) -> Self::Ref {
                match self.variant.index_as(index) {
                    #( #indices => #name::#names, )*
                    x => panic!("Unacceptable discriminant found: {:?}", x),
                }
            }
        }

        impl<CVar: ::columnar::Clear> ::columnar::Clear for #c_ident <CVar> {
            #[inline(always)]
            fn clear(&mut self) {
                self.variant.clear();
            }
        }

        impl<CVar: ::columnar::Len> ::columnar::Len for #c_ident <CVar> {
            #[inline(always)]
            fn len(&self) -> usize {
                self.variant.len()
            }
        }

        impl<'a, CVar: ::columnar::AsBytes<'a>> ::columnar::AsBytes<'a> for #c_ident <CVar> {
            const SLICE_COUNT: usize = CVar::SLICE_COUNT;
            #[inline]
            fn get_byte_slice(&self, index: usize) -> (u64, &'a [u8]) {
                debug_assert!(index < Self::SLICE_COUNT);
                self.variant.get_byte_slice(index)
            }
        }

        impl<'columnar, CVar: ::columnar::FromBytes<'columnar>> ::columnar::FromBytes<'columnar> for #c_ident <CVar> {
            const SLICE_COUNT: usize = CVar::SLICE_COUNT;
            #[inline(always)]
            fn from_bytes(bytes: &mut impl Iterator<Item=&'columnar [u8]>) -> Self {
                Self { variant: ::columnar::FromBytes::from_bytes(bytes) }
            }
            #[inline(always)]
            fn from_store(store: &::columnar::bytes::indexed::DecodedStore<'columnar>, offset: &mut usize) -> Self {
                Self { variant: ::columnar::FromBytes::from_store(store, offset) }
            }
            fn element_sizes(sizes: &mut Vec<usize>) -> ::core::result::Result<(), String> {
                CVar::element_sizes(sizes)
            }
        }

        impl ::columnar::Columnar for #name {
            #[inline(always)]
            fn copy_from<'a>(&mut self, other: ::columnar::Ref<'a, Self>) { *self = other; }
            #[inline(always)]
            fn into_owned<'a>(other: ::columnar::Ref<'a, Self>) -> Self { other }
            type Container = #c_ident;
        }

        impl<CV: ::columnar::common::BorrowIndexAs<u8>> ::columnar::Borrow for #c_ident <CV> {
            type Ref<'a> = #name;
            type Borrowed<'a> = #c_ident < CV::Borrowed<'a> > where CV: 'a;
            #[inline(always)]
            fn borrow<'a>(&'a self) -> Self::Borrowed<'a> {
                #c_ident {
                    variant: self.variant.borrow()
                }
            }
            #[inline(always)]
            fn reborrow<'b, 'a: 'b>(thing: Self::Borrowed<'a>) -> Self::Borrowed<'b> {
                #c_ident {
                    variant: <CV as ::columnar::Borrow>::reborrow(thing.variant),
                }
            }
            #[inline(always)]
            fn reborrow_ref<'b, 'a: 'b>(thing: Self::Ref<'a>) -> Self::Ref<'b> { thing }
        }

        impl<CV: ::columnar::common::PushIndexAs<u8>> ::columnar::Container for #c_ident <CV> {
            #[inline(always)]
            fn extend_from_self(&mut self, other: Self::Borrowed<'_>, range: std::ops::Range<usize>) {
                self.variant.extend_from_self(other.variant, range);
            }

            fn reserve_for<'a, I>(&mut self, selves: I) where Self: 'a, I: Iterator<Item = Self::Borrowed<'a>> + Clone {
                self.variant.reserve_for(selves.map(|x| x.variant));
            }
        }
    }.into()
}
