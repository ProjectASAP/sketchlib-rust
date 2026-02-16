//! HashLayer node adapter for the node orchestrator.
//! Wraps `HashLayer` behind the `OrchestratorNode` interface.

use crate::{HashLayer, SketchInput};

use super::{NodeQuery, OrchestratorNode};

pub struct HashLayerNode {
    inner: HashLayer,
}

impl HashLayerNode {
    pub fn new(inner: HashLayer) -> Self {
        Self { inner }
    }

    pub fn inner(&self) -> &HashLayer {
        &self.inner
    }

    pub fn inner_mut(&mut self) -> &mut HashLayer {
        &mut self.inner
    }
}

impl OrchestratorNode for HashLayerNode {
    fn kind(&self) -> &'static str {
        "HashLayer"
    }

    fn insert(&mut self, input: &SketchInput) {
        self.inner.insert_all(input);
    }

    fn query(&self, _req: &NodeQuery<'_>) -> Result<f64, &'static str> {
        Err("HashLayerNode query is TODO")
    }
}
