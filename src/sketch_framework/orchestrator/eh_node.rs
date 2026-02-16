//! Exponential-Histogram node adapter for the node orchestrator.
//! Wraps EH sketches behind the `OrchestratorNode` interface.

use crate::{
    SketchInput,
    sketch_framework::{EhSketch, ExponentialHistogram},
};

use super::{NodeInsert, NodeQuery, OrchestratorNode};

pub struct EhNode<T, Q>
where
    T: EhSketch + Clone,
    Q: Fn(&T, &NodeQuery<'_>) -> Result<f64, &'static str>,
{
    eh: ExponentialHistogram<T>,
    query_fn: Q,
}

impl<T, Q> EhNode<T, Q>
where
    T: EhSketch + Clone,
    Q: Fn(&T, &NodeQuery<'_>) -> Result<f64, &'static str>,
{
    pub fn new(eh: ExponentialHistogram<T>, query_fn: Q) -> Self {
        Self { eh, query_fn }
    }
}

impl<T, Q> OrchestratorNode for EhNode<T, Q>
where
    T: EhSketch + Clone,
    Q: Fn(&T, &NodeQuery<'_>) -> Result<f64, &'static str>,
{
    fn kind(&self) -> &'static str {
        "EH"
    }

    fn insert(&mut self, _input: &SketchInput) {
        // EH requires time; use insert_ex with NodeInsert::Eh
    }

    fn insert_ex(&mut self, input: &NodeInsert<'_>) -> Result<(), &'static str> {
        match input {
            NodeInsert::Eh { time, value } => {
                self.eh.update(*time, value);
                Ok(())
            }
            _ => Err("Input type not supported by EH node"),
        }
    }

    fn query(&self, req: &NodeQuery<'_>) -> Result<f64, &'static str> {
        match req {
            NodeQuery::EhInterval { t1, t2, query } => {
                let merged = self
                    .eh
                    .query_interval_merge(*t1, *t2)
                    .ok_or("EH has no data in interval")?;
                (self.query_fn)(&merged, query.as_ref())
            }
            _ => Err("Query type not supported by EH node"),
        }
    }
}
