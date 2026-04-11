use serde::{Deserialize, Serialize};

extern crate self as buffa_types;
pub extern crate tracing_opentelemetry;

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
        unsafe impl ::buffa::DefaultViewInstance for EmptyView<'static> {
            fn default_view_instance() -> &'static Self {
                static VALUE: EmptyView<'static> = EmptyView(::std::marker::PhantomData);
                &VALUE
            }
        }
    }
}

pub mod api;
pub mod core;
pub mod domain;
pub mod infra;
pub mod services;
pub mod startup;
pub mod metrics;
pub mod telemetry;
pub mod utils;

pub mod trading_proto {
    include!(concat!(env!("OUT_DIR"), "/_trading_include.rs"));
    pub use trading::*;
}
