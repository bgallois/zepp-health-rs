//! Small, read-only client for the reverse-engineered Zepp Cloud API.
//!
//! This crate intentionally exposes only the first Milestone 2 endpoint.  The
//! cloud API is unofficial; raw Zepp response types stay in the `api` module.

pub mod api;
pub mod health;
pub mod store;
