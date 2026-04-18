//! `trading-protocol` — Protobuf / ConnectRPC wire types.
//!
//! This crate owns the `trading.proto` definition and generated
//! ConnectRPC stubs. API and binary crates depend on this for
//! gRPC service definitions.

// The generated ConnectRPC code references `::buffa_types::google::protobuf::Empty`.
// This self-alias makes our `google::protobuf` module discoverable under that path.
extern crate self as buffa_types;

pub mod google {
    pub mod protobuf {
        use serde::{Deserialize, Serialize};

        #[derive(Clone, PartialEq, Default, Serialize, Deserialize)]
        pub struct Empty {}
        impl ::buffa::Message for Empty {
           fn compute_size(&self) -> u32 { 0 }
           fn write_to(&self, _: &mut impl ::buffa::bytes::BufMut) {}
           fn merge_field(&mut self, _: ::buffa::encoding::Tag, _: &mut impl ::buffa::bytes::Buf, _: u32) -> Result<(), ::buffa::DecodeError> { Ok(()) }
           fn cached_size(&self) -> u32 { 0 }
           fn clear(&mut self) {}
        }
        /// SAFETY: Empty is stateless — the default instance is valid for 'static.
        unsafe impl ::buffa::DefaultInstance for Empty {
            fn default_instance() -> &'static Self {
                static VALUE: Empty = Empty {};
                &VALUE
            }
        }

        #[derive(Clone, Copy, Default, Debug)]
        pub struct EmptyView<'a>(::std::marker::PhantomData<&'a ()>);
        impl<'a> ::buffa::MessageView<'a> for EmptyView<'a> {
            type Owned = Empty;
            fn decode_view(_: &'a [u8]) -> Result<Self, ::buffa::DecodeError> { Ok(EmptyView(::std::marker::PhantomData)) }
            fn decode_view_with_limit(_: &'a [u8], _: u32) -> Result<Self, ::buffa::DecodeError> { Ok(EmptyView(::std::marker::PhantomData)) }
            fn to_owned_message(&self) -> Empty { Empty {} }
        }
        /// SAFETY: EmptyView is stateless — the default instance is valid for 'static.
        unsafe impl ::buffa::DefaultViewInstance for EmptyView<'static> {
            fn default_view_instance() -> &'static Self {
                static VALUE: EmptyView<'static> = EmptyView(::std::marker::PhantomData);
                &VALUE
            }
        }
    }
}

pub mod trading_proto {
    include!(concat!(env!("OUT_DIR"), "/_trading_include.rs"));
    pub use trading::*;
}
