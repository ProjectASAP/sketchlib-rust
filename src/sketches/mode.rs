use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct RegularPath;

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct FastPath;
