//! Sketch node adapter for the node orchestrator.
//! Delegates to `OrchestratedSketch` for sketch-specific behavior.

use crate::{
    SketchInput,
    sketch_framework::{OrchestratedSketch, OrchestratorQuery},
};

use super::{NodeQuery, OrchestratorNode};

pub struct SketchNode {
    inner: OrchestratedSketch,
}

impl SketchNode {
    pub fn new(inner: OrchestratedSketch) -> Self {
        Self { inner }
    }
}

impl OrchestratorNode for SketchNode {
    fn kind(&self) -> &'static str {
        self.inner.sketch_type()
    }

    fn insert(&mut self, input: &SketchInput) {
        self.inner.insert(input);
    }

    fn query(&self, req: &NodeQuery<'_>) -> Result<f64, &'static str> {
        match req {
            NodeQuery::Sketch(val) => self.inner.query(val),
            NodeQuery::UnivMon(q) => self
                .inner
                .query_with_request(&OrchestratorQuery::UnivMon(*q)),
            NodeQuery::Hydra { key, query } => {
                self.inner.query_with_request(&OrchestratorQuery::Hydra {
                    key: key.clone(),
                    query: query.clone(),
                })
            }
            NodeQuery::Quantile { q } => match &self.inner {
                OrchestratedSketch::Quantile(sketch) => match sketch {
                    crate::sketch_framework::QuantileSketch::Kll(kll) => Ok(kll.quantile(*q)),
                    crate::sketch_framework::QuantileSketch::Dd(dd) => dd
                        .get_value_at_quantile(*q)
                        .ok_or("DDSketch has no samples"),
                },
                _ => Err("Quantile query not supported by node"),
            },
            NodeQuery::Cdf { value } => match &self.inner {
                OrchestratedSketch::Quantile(sketch) => match sketch {
                    crate::sketch_framework::QuantileSketch::Kll(kll) => {
                        Ok(kll.cdf().quantile(*value))
                    }
                    crate::sketch_framework::QuantileSketch::Dd(_) => {
                        Err("DDSketch does not support CDF query")
                    }
                },
                _ => Err("CDF query not supported by node"),
            },
            _ => Err("Query type not supported by node"),
        }
    }
}
