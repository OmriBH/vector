use std::time::Instant;

use indexmap::{IndexMap, IndexSet};
use rayon::prelude::*;
use vector_lib::id::Inputs;

use super::{
    Config, builder::ConfigBuilder, graph::Graph, transform::get_transform_output_ids, validation,
};

pub fn compile(mut builder: ConfigBuilder) -> Result<(Config, Vec<String>), Vec<String>> {
    let compile_start = Instant::now();
    let mut errors = Vec::new();

    // component names should not have dots in the configuration file
    // but components can expand (like route) to have components with a dot
    // so this check should be done before expanding components
    if let Err(name_errors) = validation::check_names(
        builder
            .transforms
            .keys()
            .chain(builder.sources.keys())
            .chain(builder.sinks.keys()),
    ) {
        errors.extend(name_errors);
    }

    let globs_start = Instant::now();
    expand_globs(&mut builder);
    info!(
        elapsed_ms = globs_start.elapsed().as_millis() as u64,
        "Glob expansion complete."
    );

    // Run validation checks in parallel for better performance with large configs
    let validation_start = Instant::now();
    let (shape_result, (resources_result, (outputs_result, alpha_result))) = rayon::join(
        || validation::check_shape(&builder),
        || {
            rayon::join(
                || validation::check_resources(&builder),
                || {
                    rayon::join(
                        || validation::check_outputs(&builder),
                        || validation::check_buffer_utilization_ewma_alpha(&builder),
                    )
                },
            )
        },
    );

    // Collect errors from parallel validation
    if let Err(type_errors) = shape_result {
        errors.extend(type_errors);
    }
    if let Err(type_errors) = resources_result {
        errors.extend(type_errors);
    }
    if let Err(output_errors) = outputs_result {
        errors.extend(output_errors);
    }
    if let Err(alpha_errors) = alpha_result {
        errors.extend(alpha_errors);
    }
    info!(
        elapsed_ms = validation_start.elapsed().as_millis() as u64,
        "Validation checks complete."
    );

    let ConfigBuilder {
        global,
        #[cfg(feature = "api")]
        api,
        schema,
        healthchecks,
        enrichment_tables,
        sources,
        sinks,
        transforms,
        tests,
        provider: _,
        secret,
        graceful_shutdown_duration,
        allow_empty: _,
    } = builder;
    let all_sinks = sinks
        .clone()
        .into_iter()
        .chain(
            enrichment_tables
                .iter()
                .filter_map(|(key, table)| table.as_sink(key)),
        )
        .collect::<IndexMap<_, _>>();
    let sources_and_table_sources = sources
        .clone()
        .into_iter()
        .chain(
            enrichment_tables
                .iter()
                .filter_map(|(key, table)| table.as_source(key)),
        )
        .collect::<IndexMap<_, _>>();

    let graph_start = Instant::now();
    let graph = match Graph::new(
        &sources_and_table_sources,
        &transforms,
        &all_sinks,
        schema,
        global.wildcard_matching.unwrap_or_default(),
    ) {
        Ok(graph) => graph,
        Err(graph_errors) => {
            errors.extend(graph_errors);
            return Err(errors);
        }
    };
    info!(
        elapsed_ms = graph_start.elapsed().as_millis() as u64,
        "Graph construction complete."
    );

    let typecheck_start = Instant::now();
    if let Err(type_errors) = graph.typecheck() {
        errors.extend(type_errors);
    }
    info!(
        elapsed_ms = typecheck_start.elapsed().as_millis() as u64,
        "Type checking complete."
    );

    let cycle_start = Instant::now();
    if let Err(e) = graph.check_for_cycles() {
        errors.push(e);
    }
    info!(
        elapsed_ms = cycle_start.elapsed().as_millis() as u64,
        "Cycle detection complete."
    );

    // Inputs are resolved from string into OutputIds as part of graph construction, so update them
    // here before adding to the final config (the types require this).
    // Use parallel iteration for better performance with large configs.
    let input_resolution_start = Instant::now();
    let sinks: IndexMap<_, _> = sinks
        .into_par_iter()
        .map(|(key, sink)| {
            let inputs = graph.inputs_for(&key);
            (key, sink.with_inputs(inputs))
        })
        .collect();
    let transforms: IndexMap<_, _> = transforms
        .into_par_iter()
        .map(|(key, transform)| {
            let inputs = graph.inputs_for(&key);
            (key, transform.with_inputs(inputs))
        })
        .collect();
    let enrichment_tables: IndexMap<_, _> = enrichment_tables
        .into_par_iter()
        .map(|(key, table)| {
            let inputs = graph.inputs_for(&key);
            (key, table.with_inputs(inputs))
        })
        .collect();
    // Tests resolution in parallel - collect results and then partition successes/failures
    let test_results: Vec<_> = tests
        .into_par_iter()
        .map(|test| test.resolve_outputs(&graph))
        .collect();

    // Partition results into successes and failures
    let mut tests = Vec::new();
    let mut test_errors: Vec<String> = Vec::new();
    for result in test_results {
        match result {
            Ok(test) => tests.push(test),
            Err(errs) => test_errors.extend(errs),
        }
    }
    if !test_errors.is_empty() {
        return Err(test_errors);
    }
    info!(
        elapsed_ms = input_resolution_start.elapsed().as_millis() as u64,
        "Input resolution complete."
    );

    info!(
        total_elapsed_ms = compile_start.elapsed().as_millis() as u64,
        "Config compilation finished."
    );

    if errors.is_empty() {
        let mut config = Config {
            global,
            #[cfg(feature = "api")]
            api,
            schema,
            healthchecks,
            enrichment_tables,
            sources,
            sinks,
            transforms,
            tests,
            secret,
            graceful_shutdown_duration,
        };

        config.propagate_acknowledgements()?;

        let warnings = validation::warnings(&config);

        Ok((config, warnings))
    } else {
        Err(errors)
    }
}

/// Expand globs in input lists
pub(crate) fn expand_globs(config: &mut ConfigBuilder) {
    // Build candidates set from component keys directly — O(1) per component.
    // This avoids calling .outputs() for 100K+ sources/transforms (which takes ~17s),
    // since the vast majority have only a default output (port: None) and their
    // candidate string is just the component key.
    let mut candidates: IndexSet<String> = IndexSet::with_capacity(
        config.sources.len() + config.transforms.len(),
    );

    for key in config.sources.keys() {
        candidates.insert(key.to_string());
    }
    for key in config.transforms.keys() {
        candidates.insert(key.to_string());
    }

    // Port-specific candidates (like "route_transform.matched") are only needed
    // if a glob pattern could match them. Port candidates contain a dot, so we
    // only do the expensive .outputs() collection if any glob contains a dot.
    let needs_port_candidates = config
        .transforms
        .values()
        .any(|t| {
            t.inputs
                .iter()
                .any(|i| is_glob_pattern(i) && i.contains('.'))
        })
        || config
            .sinks
            .values()
            .any(|s| {
                s.inputs
                    .iter()
                    .any(|i| is_glob_pattern(i) && i.contains('.'))
            });

    if needs_port_candidates {
        // Only call .outputs() for transform types known to produce named ports.
        // Most transforms (remap, filter, etc.) have a single default output
        // and their candidate is already in the key-based set.
        // This avoids calling the expensive .outputs() for 100K+ transforms.
        const MULTI_OUTPUT_TYPES: &[&str] = &["route", "exclusive_route", "remap"];

        let transform_port_candidates: Vec<String> = config
            .transforms
            .par_iter()
            .filter(|(_, t)| {
                let name = t.inner.get_component_name();
                MULTI_OUTPUT_TYPES.contains(&name)
            })
            .flat_map(|(key, t)| {
                get_transform_output_ids(
                    t.inner.as_ref(),
                    key.clone(),
                    config.schema.log_namespace(),
                )
                .filter(|output_id| output_id.port.is_some())
                .map(|output_id| output_id.to_string())
                .collect::<Vec<_>>()
            })
            .collect();

        if !transform_port_candidates.is_empty() {
            info!(
                count = transform_port_candidates.len(),
                "Added port-specific candidates from multi-output transforms."
            );
        }

        for candidate in transform_port_candidates {
            candidates.insert(candidate);
        }
    }

    // Expand globs in parallel — each component's input list is independent
    config
        .transforms
        .par_iter_mut()
        .for_each(|(id, transform)| {
            expand_globs_inner(&mut transform.inputs, &id.to_string(), &candidates);
        });

    config.sinks.par_iter_mut().for_each(|(id, sink)| {
        expand_globs_inner(&mut sink.inputs, &id.to_string(), &candidates);
    });
}

enum InputMatcher {
    Pattern(glob::Pattern),
    String(String),
}

impl InputMatcher {
    fn matches(&self, candidate: &str) -> bool {
        use InputMatcher::*;

        match self {
            Pattern(pattern) => pattern.matches(candidate),
            String(s) => s == candidate,
        }
    }
}

/// Returns true if the string contains glob metacharacters (*, ?, [).
fn is_glob_pattern(s: &str) -> bool {
    s.contains('*') || s.contains('?') || s.contains('[')
}

fn expand_globs_inner(inputs: &mut Inputs<String>, id: &str, candidates: &IndexSet<String>) {
    let raw_inputs = std::mem::take(inputs);
    for raw_input in raw_inputs {
        // Fast path: if the input is a literal (no glob chars), do an O(1) set lookup
        // instead of iterating through all candidates. This is the common case.
        if !is_glob_pattern(&raw_input) {
            if candidates.contains(&raw_input) && raw_input != id {
                inputs.extend(Some(raw_input));
            } else {
                // Leave unmatched literals as-is for better error messages downstream
                inputs.extend(Some(raw_input));
            }
            continue;
        }

        // Slow path: actual glob pattern — must iterate candidates
        let matcher = glob::Pattern::new(&raw_input)
            .map(InputMatcher::Pattern)
            .unwrap_or_else(|error| {
                warn!(message = "Invalid glob pattern for input.", component_id = %id, %error);
                InputMatcher::String(raw_input.to_string())
            });
        let mut matched = false;
        for input in candidates {
            if matcher.matches(input) && input != id {
                matched = true;
                inputs.extend(Some(input.to_string()))
            }
        }
        // If it didn't work as a glob pattern, leave it in the inputs as-is. This lets us give
        // more accurate error messages about nonexistent inputs.
        if !matched {
            inputs.extend(Some(raw_input))
        }
    }
}

#[cfg(test)]
mod test {
    use vector_lib::config::ComponentKey;

    use super::*;
    use crate::test_util::mock::{basic_sink, basic_source, basic_transform};

    #[test]
    fn glob_expansion() {
        let mut builder = ConfigBuilder::default();
        builder.add_source("foo1", basic_source().1);
        builder.add_source("foo2", basic_source().1);
        builder.add_source("bar", basic_source().1);
        builder.add_transform("foos", &["foo*"], basic_transform("", 1.0));
        builder.add_sink("baz", &["foos*", "b*"], basic_sink(1).1);
        builder.add_sink("quix", &["*oo*"], basic_sink(1).1);
        builder.add_sink("quux", &["*"], basic_sink(1).1);

        let config = builder.build().expect("build should succeed");

        assert_eq!(
            config
                .transforms
                .get(&ComponentKey::from("foos"))
                .map(|item| without_ports(item.inputs.clone()))
                .unwrap(),
            vec![ComponentKey::from("foo1"), ComponentKey::from("foo2")]
        );
        assert_eq!(
            config
                .sinks
                .get(&ComponentKey::from("baz"))
                .map(|item| without_ports(item.inputs.clone()))
                .unwrap(),
            vec![ComponentKey::from("foos"), ComponentKey::from("bar")]
        );
        assert_eq!(
            config
                .sinks
                .get(&ComponentKey::from("quux"))
                .map(|item| without_ports(item.inputs.clone()))
                .unwrap(),
            vec![
                ComponentKey::from("foo1"),
                ComponentKey::from("foo2"),
                ComponentKey::from("bar"),
                ComponentKey::from("foos")
            ]
        );
        assert_eq!(
            config
                .sinks
                .get(&ComponentKey::from("quix"))
                .map(|item| without_ports(item.inputs.clone()))
                .unwrap(),
            vec![
                ComponentKey::from("foo1"),
                ComponentKey::from("foo2"),
                ComponentKey::from("foos")
            ]
        );
    }

    /// Test that parallel compilation works correctly with many components.
    /// This exercises the parallel validation, node creation, and input resolution.
    #[test]
    fn parallel_compilation_many_components() {
        let mut builder = ConfigBuilder::default();

        // Create many sources
        let num_sources = 100;
        for i in 0..num_sources {
            builder.add_source(&format!("source_{i}"), basic_source().1);
        }

        // Create many transforms, each taking input from a source
        let num_transforms = 100;
        for i in 0..num_transforms {
            let input = format!("source_{}", i % num_sources);
            builder.add_transform(
                &format!("transform_{i}"),
                &[&input],
                basic_transform("", 1.0),
            );
        }

        // Create many sinks, each taking input from a transform
        let num_sinks = 100;
        for i in 0..num_sinks {
            let input = format!("transform_{}", i % num_transforms);
            builder.add_sink(&format!("sink_{i}"), &[&input], basic_sink(1).1);
        }

        // Compile the config - this exercises parallel processing
        let config = builder.build().expect("build should succeed");

        // Verify all components are present
        assert_eq!(config.sources.len(), num_sources);
        assert_eq!(config.transforms.len(), num_transforms);
        assert_eq!(config.sinks.len(), num_sinks);

        // Verify inputs are correctly resolved
        for i in 0..num_sinks {
            let sink = config
                .sinks
                .get(&ComponentKey::from(format!("sink_{i}")))
                .expect("sink should exist");
            assert_eq!(sink.inputs.len(), 1, "sink_{i} should have exactly 1 input");
        }
    }

    /// Test that parallel compilation produces correct results with complex pipelines.
    #[test]
    fn parallel_compilation_complex_pipeline() {
        let mut builder = ConfigBuilder::default();

        // Create a fan-out / fan-in pattern
        builder.add_source("main_source", basic_source().1);

        // Fan-out: multiple transforms from one source
        for i in 0..10 {
            builder.add_transform(
                &format!("fanout_{i}"),
                &["main_source"],
                basic_transform("", 1.0),
            );
        }

        // Fan-in: one sink from multiple transforms
        let inputs: Vec<String> = (0..10).map(|i| format!("fanout_{i}")).collect();
        let input_refs: Vec<&str> = inputs.iter().map(|s| s.as_str()).collect();
        builder.add_sink("final_sink", &input_refs, basic_sink(1).1);

        let config = builder.build().expect("build should succeed");

        assert_eq!(config.sources.len(), 1);
        assert_eq!(config.transforms.len(), 10);
        assert_eq!(config.sinks.len(), 1);

        // Verify the final sink has all 10 inputs
        let final_sink = config
            .sinks
            .get(&ComponentKey::from("final_sink"))
            .expect("final_sink should exist");
        assert_eq!(
            final_sink.inputs.len(),
            10,
            "final_sink should have 10 inputs"
        );
    }

    fn without_ports(outputs: Inputs<OutputId>) -> Vec<ComponentKey> {
        outputs
            .into_iter()
            .map(|output| {
                assert!(output.port.is_none());
                output.component
            })
            .collect()
    }
}
