//! Node-level orchestration API that unifies sketches and higher-level frameworks (EH/Nitro).
//! Provides a common `OrchestratorNode` interface, node queries/inserts, and a selector-based manager.
//! Experimental/in-progress: API shape may change in upcoming releases.

use crate::{SketchInput, input::HydraQuery, sketch_framework::UnivMonQuery};

pub trait OrchestratorNode {
    fn kind(&self) -> &'static str;
    fn insert(&mut self, input: &SketchInput);
    fn insert_ex(&mut self, input: &NodeInsert<'_>) -> Result<(), &'static str> {
        match input {
            NodeInsert::Sketch(val) => {
                self.insert(val);
                Ok(())
            }
            _ => Err("Input type not supported by node"),
        }
    }
    fn query(&self, req: &NodeQuery<'_>) -> Result<f64, &'static str>;
}

#[derive(Clone, Debug)]
pub struct NodeMeta {
    pub name: String,
    pub tags: Vec<String>,
}

impl NodeMeta {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            tags: Vec::new(),
        }
    }

    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.tags = tags;
        self
    }
}

pub enum NodeSelector<'a> {
    All,
    Indices(&'a [usize]),
    Names(&'a [&'a str]),
    Tags(&'a [&'a str]),
    Kinds(&'a [&'a str]),
}

pub enum NodeInsert<'a> {
    Sketch(&'a SketchInput<'a>),
    Eh {
        time: u64,
        value: SketchInput<'static>,
    },
    NitroBatch {
        data: &'a [i64],
    },
}

pub enum NodeQuery<'a> {
    Sketch(&'a SketchInput<'a>),
    Quantile {
        q: f64,
    },
    Cdf {
        value: f64,
    },
    UnivMon(UnivMonQuery),
    Hydra {
        key: Vec<&'a str>,
        query: HydraQuery<'a>,
    },
    EhInterval {
        t1: u64,
        t2: u64,
        query: Box<NodeQuery<'a>>,
    },
    NitroEstimate {
        input: &'a SketchInput<'a>,
    },
}

pub struct Orchestrator {
    nodes: Vec<Box<dyn OrchestratorNode>>,
    metas: Vec<NodeMeta>,
}

impl Orchestrator {
    pub fn new(nodes: Vec<(Box<dyn OrchestratorNode>, NodeMeta)>) -> Self {
        let (nodes, metas): (Vec<_>, Vec<_>) = nodes.into_iter().unzip();
        Self { nodes, metas }
    }

    pub fn register(&mut self, node: Box<dyn OrchestratorNode>, meta: NodeMeta) -> usize {
        let index = self.nodes.len();
        self.nodes.push(node);
        self.metas.push(meta);
        index
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    pub fn meta(&self, index: usize) -> Option<&NodeMeta> {
        self.metas.get(index)
    }

    pub fn insert(&mut self, selector: NodeSelector<'_>, input: &SketchInput) {
        let _ = self.insert_ex(selector, &NodeInsert::Sketch(input));
    }

    pub fn insert_ex(
        &mut self,
        selector: NodeSelector<'_>,
        input: &NodeInsert<'_>,
    ) -> Vec<(usize, Result<(), &'static str>)> {
        let indices = self.resolve_indices(selector);
        indices
            .into_iter()
            .map(|idx| (idx, self.nodes[idx].insert_ex(input)))
            .collect()
    }

    pub fn query(
        &self,
        selector: NodeSelector<'_>,
        req: &NodeQuery<'_>,
    ) -> Vec<(usize, Result<f64, &'static str>)> {
        let indices = self.resolve_indices(selector);
        indices
            .into_iter()
            .map(|idx| (idx, self.nodes[idx].query(req)))
            .collect()
    }

    pub fn query_one(
        &self,
        selector: NodeSelector<'_>,
        req: &NodeQuery<'_>,
    ) -> Result<(usize, Result<f64, &'static str>), &'static str> {
        let indices = self.resolve_indices(selector);
        if indices.is_empty() {
            return Err("No nodes matched");
        }
        if indices.len() > 1 {
            return Err("Multiple nodes matched");
        }
        let idx = indices[0];
        Ok((idx, self.nodes[idx].query(req)))
    }

    fn resolve_indices(&self, selector: NodeSelector<'_>) -> Vec<usize> {
        let len = self.nodes.len();
        let mut selected = vec![false; len];

        match selector {
            NodeSelector::All => selected.iter_mut().for_each(|s| *s = true),
            NodeSelector::Indices(indices) => {
                for &idx in indices {
                    if idx < len {
                        selected[idx] = true;
                    }
                }
            }
            NodeSelector::Names(names) => {
                for (idx, meta) in self.metas.iter().enumerate() {
                    for &name in names {
                        if meta.name == name {
                            selected[idx] = true;
                            break;
                        }
                    }
                }
            }
            NodeSelector::Tags(tags) => {
                for (idx, meta) in self.metas.iter().enumerate() {
                    if meta.tags.iter().any(|tag| tags.iter().any(|t| *t == tag)) {
                        selected[idx] = true;
                    }
                }
            }
            NodeSelector::Kinds(kinds) => {
                for idx in 0..len {
                    let kind = self.nodes[idx].kind();
                    if kinds.iter().any(|k| *k == kind) {
                        selected[idx] = true;
                    }
                }
            }
        }

        selected
            .into_iter()
            .enumerate()
            .filter_map(|(idx, is_selected)| if is_selected { Some(idx) } else { None })
            .collect()
    }
}
