//! Nitro node adapter for the node orchestrator.
//! Wraps Nitro batch processing behind the `OrchestratorNode` interface.

use crate::{
    SketchInput,
    sketch_framework::{NitroBatch, NitroEstimate, NitroTarget},
};

use super::{NodeInsert, NodeQuery, OrchestratorNode};

pub struct NitroNode<S>
where
    S: NitroTarget + NitroEstimate,
{
    nitro: NitroBatch<S>,
}

impl<S> NitroNode<S>
where
    S: NitroTarget + NitroEstimate,
{
    pub fn new(nitro: NitroBatch<S>) -> Self {
        Self { nitro }
    }
}

impl<S> OrchestratorNode for NitroNode<S>
where
    S: NitroTarget + NitroEstimate,
{
    fn kind(&self) -> &'static str {
        "Nitro"
    }

    fn insert(&mut self, _input: &SketchInput) {
        // Nitro expects batches; use insert_ex with NodeInsert::NitroBatch
    }

    fn insert_ex(&mut self, input: &NodeInsert<'_>) -> Result<(), &'static str> {
        match input {
            NodeInsert::NitroBatch { data } => {
                self.nitro.insert(data);
                Ok(())
            }
            _ => Err("Input type not supported by Nitro node"),
        }
    }

    fn query(&self, req: &NodeQuery<'_>) -> Result<f64, &'static str> {
        match req {
            NodeQuery::NitroEstimate { input } => Ok(self.nitro.estimate_median(input)),
            _ => Err("Query type not supported by Nitro node"),
        }
    }
}
