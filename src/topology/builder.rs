use std::{
    collections::{HashMap, HashSet},
    future::ready,
    num::NonZeroUsize,
    sync::{Arc, LazyLock, Mutex},
    time::Instant,
};

use futures::{FutureExt, StreamExt, TryStreamExt, stream::FuturesOrdered};
use futures_util::stream::FuturesUnordered;
use metrics::gauge;
use stream_cancel::{StreamExt as StreamCancelExt, Trigger, Tripwire};
use tokio::{
    select,
    sync::{mpsc::UnboundedSender, oneshot},
    time::timeout,
};
use tracing::Instrument;
use vector_lib::{
    EstimatedJsonEncodedSizeOf,
    buffers::{
        BufferType, WhenFull,
        topology::{
            builder::TopologyBuilder,
            channel::{BufferReceiver, BufferSender, ChannelMetricMetadata},
        },
    },
    internal_event::{self, CountByteSize, EventsSent, InternalEventHandle as _, Registered},
    schema::Definition,
    source_sender::{CHUNK_SIZE, SourceSenderItem},
    transform::update_runtime_schema_definition,
};
use vector_vrl_metrics::MetricsStorage;

use super::{
    BuiltBuffer, ConfigDiff,
    fanout::{self, Fanout},
    schema::{self, ComponentContainer},
    task::{Task, TaskOutput, TaskResult},
};
use crate::{
    SourceSender,
    config::{
        ComponentKey, Config, DataType, EnrichmentTableConfig, Input, Inputs, OutputId,
        ProxyConfig, SinkContext, SourceContext, TransformContext, TransformOuter, TransformOutput,
    },
    event::{EventArray, EventContainer},
    extra_context::ExtraContext,
    internal_events::EventsReceived,
    shutdown::SourceShutdownCoordinator,
    spawn_named,
    topology::task::TaskError,
    transforms::{SyncTransform, TaskTransform, Transform, TransformOutputs, TransformOutputsBuf},
    utilization::{UtilizationComponentSender, UtilizationEmitter, UtilizationRegistry, wrap},
};

static ENRICHMENT_TABLES: LazyLock<vector_lib::enrichment::TableRegistry> =
    LazyLock::new(vector_lib::enrichment::TableRegistry::default);
static METRICS_STORAGE: LazyLock<MetricsStorage> = LazyLock::new(MetricsStorage::default);

pub(crate) static SOURCE_SENDER_BUFFER_SIZE: LazyLock<usize> =
    LazyLock::new(|| *TRANSFORM_CONCURRENCY_LIMIT * CHUNK_SIZE);

const READY_ARRAY_CAPACITY: NonZeroUsize = NonZeroUsize::new(CHUNK_SIZE * 4).unwrap();
pub(crate) const TOPOLOGY_BUFFER_SIZE: NonZeroUsize = NonZeroUsize::new(100).unwrap();
const TRANSFORM_CHANNEL_METRIC_PREFIX: &str = "transform_buffer";

static TRANSFORM_CONCURRENCY_LIMIT: LazyLock<usize> = LazyLock::new(|| {
    crate::app::worker_threads()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or_else(crate::num_threads)
});

const INTERNAL_SOURCES: [&str; 2] = ["internal_logs", "internal_metrics"];

struct Builder<'a> {
    config: &'a super::Config,
    diff: &'a ConfigDiff,
    shutdown_coordinator: SourceShutdownCoordinator,
    errors: Vec<String>,
    outputs: HashMap<OutputId, UnboundedSender<fanout::ControlMessage>>,
    tasks: HashMap<ComponentKey, Task>,
    buffers: HashMap<ComponentKey, BuiltBuffer>,
    inputs: HashMap<ComponentKey, (BufferSender<EventArray>, Inputs<OutputId>)>,
    healthchecks: HashMap<ComponentKey, Task>,
    detach_triggers: HashMap<ComponentKey, Trigger>,
    extra_context: ExtraContext,
    utilization_emitter: Option<UtilizationEmitter>,
    utilization_registry: UtilizationRegistry,
}

impl<'a> Builder<'a> {
    fn new(
        config: &'a super::Config,
        diff: &'a ConfigDiff,
        buffers: HashMap<ComponentKey, BuiltBuffer>,
        extra_context: ExtraContext,
        utilization_registry: Option<UtilizationRegistry>,
    ) -> Self {
        // If registry is not passed, we need to build a whole new utilization emitter + registry
        // Otherwise, we just store the registry and reuse it for this build
        let (emitter, registry) = if let Some(registry) = utilization_registry {
            (None, registry)
        } else {
            let (emitter, registry) = UtilizationEmitter::new();
            (Some(emitter), registry)
        };
        Self {
            config,
            diff,
            buffers,
            shutdown_coordinator: SourceShutdownCoordinator::default(),
            errors: vec![],
            outputs: HashMap::new(),
            tasks: HashMap::new(),
            inputs: HashMap::new(),
            healthchecks: HashMap::new(),
            detach_triggers: HashMap::new(),
            extra_context,
            utilization_emitter: emitter,
            utilization_registry: registry,
        }
    }

    /// Builds the new pieces of the topology found in `self.diff`.
    async fn build(mut self) -> Result<TopologyPieces, Vec<String>> {
        let enrichment_tables = self.load_enrichment_tables().await;
        let source_tasks = self.build_sources(enrichment_tables).await;
        self.build_transforms(enrichment_tables).await;
        self.build_sinks(enrichment_tables).await;

        // We should have all the data for the enrichment tables loaded now, so switch them over to
        // readonly.
        enrichment_tables.finish_load();

        if self.errors.is_empty() {
            Ok(TopologyPieces {
                inputs: self.inputs,
                outputs: Self::finalize_outputs(self.outputs),
                tasks: self.tasks,
                source_tasks,
                healthchecks: self.healthchecks,
                shutdown_coordinator: self.shutdown_coordinator,
                detach_triggers: self.detach_triggers,
                metrics_storage: METRICS_STORAGE.clone(),
                utilization: self
                    .utilization_emitter
                    .map(|e| (e, self.utilization_registry)),
            })
        } else {
            Err(self.errors)
        }
    }

    fn finalize_outputs(
        outputs: HashMap<OutputId, UnboundedSender<fanout::ControlMessage>>,
    ) -> HashMap<ComponentKey, HashMap<Option<String>, UnboundedSender<fanout::ControlMessage>>>
    {
        let mut finalized_outputs = HashMap::new();
        for (id, output) in outputs {
            let entry = finalized_outputs
                .entry(id.component)
                .or_insert_with(HashMap::new);
            entry.insert(id.port, output);
        }

        finalized_outputs
    }

    /// Loads, or reloads the enrichment tables.
    /// The tables are stored in the `ENRICHMENT_TABLES` global variable.
    async fn load_enrichment_tables(&mut self) -> &'static vector_lib::enrichment::TableRegistry {
        let mut enrichment_tables = HashMap::new();

        // Build enrichment tables
        'tables: for (name, table_outer) in self.config.enrichment_tables.iter() {
            let table_name = name.to_string();
            if ENRICHMENT_TABLES.needs_reload(&table_name) {
                let indexes = if !self.diff.enrichment_tables.is_added(name) {
                    // If this is an existing enrichment table, we need to store the indexes to reapply
                    // them again post load.
                    Some(ENRICHMENT_TABLES.index_fields(&table_name))
                } else {
                    None
                };

                let mut table = match table_outer.inner.build(&self.config.global).await {
                    Ok(table) => table,
                    Err(error) => {
                        self.errors
                            .push(format!("Enrichment Table \"{name}\": {error}"));
                        continue;
                    }
                };

                if let Some(indexes) = indexes {
                    for (case, index) in indexes {
                        match table
                            .add_index(case, &index.iter().map(|s| s.as_ref()).collect::<Vec<_>>())
                        {
                            Ok(_) => (),
                            Err(error) => {
                                // If there is an error adding an index we do not want to use the reloaded
                                // data, the previously loaded data will still need to be used.
                                // Just report the error and continue.
                                error!(message = "Unable to add index to reloaded enrichment table.",
                                    table = ?name.to_string(),
                                    %error);
                                continue 'tables;
                            }
                        }
                    }
                }

                enrichment_tables.insert(table_name, table);
            }
        }

        ENRICHMENT_TABLES.load(enrichment_tables);

        &ENRICHMENT_TABLES
    }

    async fn build_sources(
        &mut self,
        enrichment_tables: &vector_lib::enrichment::TableRegistry,
    ) -> HashMap<ComponentKey, Task> {
        let mut source_tasks = HashMap::new();

        let table_sources = self
            .config
            .enrichment_tables
            .iter()
            .filter_map(|(key, table)| table.as_source(key))
            .collect::<Vec<_>>();
        for (key, source) in self
            .config
            .sources()
            .filter(|(key, _)| self.diff.sources.contains_new(key))
            .chain(
                table_sources
                    .iter()
                    .map(|(key, source)| (key, source))
                    .filter(|(key, _)| self.diff.enrichment_tables.contains_new(key)),
            )
        {
            debug!(component_id = %key, "Building new source.");

            let typetag = source.inner.get_component_name();
            let source_outputs = source.inner.outputs(self.config.schema.log_namespace());

            let span = error_span!(
                "source",
                component_kind = "source",
                component_id = %key.id(),
                component_type = %source.inner.get_component_name(),
            );
            let _entered_span = span.enter();

            let task_name = format!(
                ">> {} ({}, pump) >>",
                source.inner.get_component_name(),
                key.id()
            );

            let mut builder = SourceSender::builder()
                .with_buffer(*SOURCE_SENDER_BUFFER_SIZE)
                .with_timeout(source.inner.send_timeout())
                .with_ewma_alpha(self.config.global.buffer_utilization_ewma_alpha);
            let mut pumps = Vec::new();
            let mut controls = HashMap::new();
            let mut schema_definitions = HashMap::with_capacity(source_outputs.len());

            for output in source_outputs.into_iter() {
                let mut rx = builder.add_source_output(output.clone(), key.clone());

                let (mut fanout, control) = Fanout::new();
                let source_type = source.inner.get_component_name();
                let source = Arc::new(key.clone());

                let pump = async move {
                    debug!("Source pump starting.");

                    while let Some(SourceSenderItem {
                        events: mut array,
                        send_reference,
                    }) = rx.next().await
                    {
                        array.set_output_id(&source);
                        array.set_source_type(source_type);
                        fanout
                            .send(array, Some(send_reference))
                            .await
                            .map_err(|e| {
                                debug!("Source pump finished with an error.");
                                TaskError::wrapped(e)
                            })?;
                    }

                    debug!("Source pump finished normally.");
                    Ok(TaskOutput::Source)
                };

                pumps.push(pump.instrument(span.clone()));
                controls.insert(
                    OutputId {
                        component: key.clone(),
                        port: output.port.clone(),
                    },
                    control,
                );

                let port = output.port.clone();
                if let Some(definition) = output.schema_definition(self.config.schema.enabled) {
                    schema_definitions.insert(port, definition);
                }
            }

            let (pump_error_tx, mut pump_error_rx) = oneshot::channel();
            let pump = async move {
                debug!("Source pump supervisor starting.");

                // Spawn all of the per-output pumps and then await their completion.
                //
                // If any of the pumps complete with an error, or panic/are cancelled, we return
                // immediately.
                let mut handles = FuturesUnordered::new();
                for pump in pumps {
                    handles.push(spawn_named(pump, task_name.as_ref()));
                }

                let mut had_pump_error = false;
                while let Some(output) = handles.try_next().await? {
                    if let Err(e) = output {
                        // Immediately send the error to the source's wrapper future, but ignore any
                        // errors during the send, since nested errors wouldn't make any sense here.
                        _ = pump_error_tx.send(e);
                        had_pump_error = true;
                        break;
                    }
                }

                if had_pump_error {
                    debug!("Source pump supervisor task finished with an error.");
                } else {
                    debug!("Source pump supervisor task finished normally.");
                }
                Ok(TaskOutput::Source)
            };
            let pump = Task::new(key.clone(), typetag, pump);

            let (shutdown_signal, force_shutdown_tripwire) = self
                .shutdown_coordinator
                .register_source(key, INTERNAL_SOURCES.contains(&typetag));

            let context = SourceContext {
                key: key.clone(),
                globals: self.config.global.clone(),
                enrichment_tables: enrichment_tables.clone(),
                metrics_storage: METRICS_STORAGE.clone(),
                shutdown: shutdown_signal,
                out: builder.build(),
                proxy: ProxyConfig::merge_with_env(&self.config.global.proxy, &source.proxy),
                acknowledgements: source.sink_acknowledgements,
                schema_definitions,
                schema: self.config.schema,
                extra_context: self.extra_context.clone(),
            };
            let server = match source.inner.build(context).await {
                Err(error) => {
                    self.errors.push(format!("Source \"{key}\": {error}"));
                    continue;
                }
                Ok(server) => server,
            };

            // Build a wrapper future that drives the actual source future, but returns early if we've
            // been signalled to forcefully shutdown, or if the source pump encounters an error.
            //
            // The forceful shutdown will only resolve if the source itself doesn't shutdown gracefully
            // within the allotted time window. This can occur normally for certain sources, like stdin,
            // where the I/O is blocking (in a separate thread) and won't wake up to check if it's time
            // to shutdown unless some input is given.
            let server = async move {
                debug!("Source starting.");

                let mut result = select! {
                    biased;

                    // We've been told that we must forcefully shut down.
                    _ = force_shutdown_tripwire => Ok(()),

                    // The source pump encountered an error, which we're now bubbling up here to stop
                    // the source as well, since the source running makes no sense without the pump.
                    //
                    // We only match receiving a message, not the error of the sender being dropped,
                    // just to keep things simpler.
                    Ok(e) = &mut pump_error_rx => Err(e),

                    // The source finished normally.
                    result = server => result.map_err(|_| TaskError::Opaque),
                };

                // Even though we already tried to receive any pump task error above, we may have exited
                // on the source itself returning an error due to task scheduling, where the pump task
                // encountered an error, sent it over the oneshot, but we were polling the source
                // already and hit an error trying to send to the now-shutdown pump task.
                //
                // Since the error from the source is opaque at the moment (i.e. `()`), we try a final
                // time to see if the pump task encountered an error, using _that_ instead if so, to
                // propagate the true error that caused the source to have to stop.
                if let Ok(e) = pump_error_rx.try_recv() {
                    result = Err(e);
                }

                match result {
                    Ok(()) => {
                        debug!("Source finished normally.");
                        Ok(TaskOutput::Source)
                    }
                    Err(e) => {
                        debug!("Source finished with an error.");
                        Err(e)
                    }
                }
            };
            let server = Task::new(key.clone(), typetag, server);

            self.outputs.extend(controls);
            self.tasks.insert(key.clone(), pump);
            source_tasks.insert(key.clone(), server);
        }

        source_tasks
    }

    async fn build_transforms(
        &mut self,
        enrichment_tables: &vector_lib::enrichment::TableRegistry,
    ) {
        use rayon::prelude::*;

        let build_transforms_start = Instant::now();

        // Collect all transforms that need processing
        let transforms_to_process: Vec<_> = self
            .config
            .transforms()
            .filter(|(key, _)| self.diff.transforms.contains_new(key))
            .collect();

        info!(
            transform_count = transforms_to_process.len(),
            "Starting transform build pipeline."
        );

        // --- Schema definition resolution (topological layer processing) ---
        // Processes transforms in dependency order: layer 0 = transforms whose inputs
        // are all sources, layer 1 = transforms whose inputs are all sources or layer-0
        // transforms, etc. Within each layer, all transforms are processed in parallel.
        // This guarantees cache hits for all upstream dependencies, eliminating redundant
        // recursive computation that caused 36s latency when all transforms raced at once.
        let schema_start = Instant::now();
        let concurrent_cache = schema::ConcurrentCache::new(HashMap::default());
        let outputs_cache = schema::TransformOutputsCache::new(HashMap::default());
        let config = self.config;
        let enrichment_tables_for_par = enrichment_tables.clone();

        // Build a set of all source keys for fast lookup
        let source_keys: HashSet<&crate::config::ComponentKey> =
            config.sources().map(|(k, _)| k).collect();

        // Build a set of all transform keys involved
        let transform_keys: HashSet<&crate::config::ComponentKey> = transforms_to_process
            .iter()
            .map(|(key, _)| *key)
            .collect();

        // Assign each transform to a topological layer
        // Layer 0: all inputs are sources (or non-transforms)
        // Layer N: all inputs are sources or transforms in layers 0..N-1
        let mut transform_layer: HashMap<&crate::config::ComponentKey, usize> =
            HashMap::with_capacity(transforms_to_process.len());
        let mut assigned = 0usize;
        let total = transforms_to_process.len();
        let mut current_layer = 0usize;

        while assigned < total {
            let mut newly_assigned = Vec::new();
            for &(key, transform) in &transforms_to_process {
                if transform_layer.contains_key(key) {
                    continue;
                }
                // Check if all input components are either sources or already-assigned transforms
                let all_deps_resolved = transform.inputs.iter().all(|input| {
                    let comp = &input.component;
                    source_keys.contains(comp)
                        || !transform_keys.contains(comp)
                        || transform_layer.contains_key(comp)
                });
                if all_deps_resolved {
                    newly_assigned.push(key);
                }
            }
            if newly_assigned.is_empty() {
                // Remaining transforms have circular deps or unresolvable inputs;
                // fall back to processing them all at once (cache will still help)
                for &(key, _) in &transforms_to_process {
                    if !transform_layer.contains_key(key) {
                        transform_layer.insert(key, current_layer);
                        assigned += 1;
                    }
                }
            } else {
                for key in &newly_assigned {
                    transform_layer.insert(*key, current_layer);
                    assigned += 1;
                }
                current_layer += 1;
            }
        }

        let num_layers = current_layer;

        // Build a lookup map from pre-computed results
        let mut definitions_map: HashMap<
            crate::config::ComponentKey,
            Vec<(crate::config::OutputId, Definition)>,
        > = HashMap::with_capacity(transforms_to_process.len());

        // Process layer by layer — within each layer, all transforms run in parallel.
        // After each layer, pre-compute outputs for that layer's transforms so
        // the NEXT layer finds them in cache (eliminating thundering herd / cache stampede).
        let mut schema_error = false;
        for layer in 0..num_layers {
            let layer_transforms: Vec<_> = transforms_to_process
                .iter()
                .filter(|(key, _)| transform_layer.get(key) == Some(&layer))
                .collect();

            debug!(
                layer = layer,
                count = layer_transforms.len(),
                "Processing schema layer."
            );

            // Step 1: Resolve input definitions for all transforms in this layer.
            // For layer 0, inputs are sources (no outputs() needed).
            // For layer N>0, all upstream outputs are pre-cached from previous layers.
            let layer_results: Vec<_> = layer_transforms
                .par_iter()
                .map(|&&(key, transform)| {
                    let result = schema::input_definitions_concurrent(
                        &transform.inputs,
                        config,
                        enrichment_tables_for_par.clone(),
                        &concurrent_cache,
                        &outputs_cache,
                    );
                    (key.clone(), result)
                })
                .collect();

            for (key, result) in layer_results {
                match result {
                    Ok(definitions) => {
                        definitions_map.insert(key, definitions);
                    }
                    Err(_) => {
                        schema_error = true;
                    }
                }
            }

            if schema_error {
                return;
            }

            // Step 2: Pre-compute outputs for this layer's transforms in parallel.
            // This ensures the next layer gets instant cache hits when it references
            // these transforms as inputs, instead of N threads racing to compute
            // the same outputs simultaneously.
            let layer_keys: Vec<_> = layer_transforms
                .iter()
                .map(|&&(key, _)| key)
                .collect();

            layer_keys.par_iter().for_each(|key| {
                // Only compute if not already cached (shouldn't be, but safe check)
                {
                    let guard = outputs_cache.read().unwrap();
                    if guard.contains_key(key) {
                        return;
                    }
                }
                if let Some(input_defs) = definitions_map.get(key) {
                    if let Some(outputs) =
                        config.transform_outputs(key, enrichment_tables_for_par.clone(), input_defs)
                    {
                        let mut guard = outputs_cache.write().unwrap();
                        guard.insert((*key).clone(), outputs);
                    }
                }
            });
        }

        info!(
            elapsed_ms = schema_start.elapsed().as_millis() as u64,
            layers = num_layers,
            "Schema definition resolution complete."
        );

        // --- Transform context preparation (parallel via rayon) ---
        // Builds TransformContext, TransformNode, and schema_definitions for each transform.
        let preparation_start = Instant::now();
        let config_schema = self.config.schema;
        let config_global = &self.config.global;
        let extra_context = &self.extra_context;

        let prepared_transforms: Vec<_> = transforms_to_process
            .par_iter()
            .map(|&(key, transform)| {
                debug!(component_id = %key, "Preparing transform.");

                let input_definitions = definitions_map
                    .get(key)
                    .cloned()
                    .unwrap_or_default();

                let merged_definition: Definition = input_definitions
                    .iter()
                    .map(|(_output_id, definition)| definition.clone())
                    .reduce(Definition::merge)
                    .unwrap_or_else(Definition::any);

                let span = error_span!(
                    "transform",
                    component_kind = "transform",
                    component_id = %key.id(),
                    component_type = %transform.inner.get_component_name(),
                );

                // Create a map of the outputs to the list of possible definitions.
                // Try the outputs cache first (populated during schema resolution),
                // falling back to computing if not cached.
                let transform_outputs = schema::get_cached_transform_outputs(
                    &outputs_cache,
                    key,
                ).unwrap_or_else(|| {
                    transform.inner.outputs(
                        &TransformContext {
                            enrichment_tables: enrichment_tables.clone(),
                            metrics_storage: METRICS_STORAGE.clone(),
                            schema: config_schema,
                            ..Default::default()
                        },
                        &input_definitions,
                    )
                });

                let schema_definitions = transform_outputs
                    .into_iter()
                    .map(|output| {
                        let definitions = output.schema_definitions(config_schema.enabled);
                        (output.port, definitions)
                    })
                    .collect::<HashMap<_, _>>();

                let context = TransformContext {
                    key: Some(key.clone()),
                    globals: config_global.clone(),
                    enrichment_tables: enrichment_tables.clone(),
                    metrics_storage: METRICS_STORAGE.clone(),
                    schema_definitions,
                    merged_schema_definition: merged_definition.clone(),
                    schema: config_schema,
                    extra_context: extra_context.clone(),
                };

                let node =
                    TransformNode::from_parts(key.clone(), &context, transform, &input_definitions);

                // Clone the inner transform config so we can move it into a spawned task.
                let inner_clone = transform.inner.clone();

                (key.clone(), inner_clone, context, node, span)
            })
            .collect();

        info!(
            elapsed_ms = preparation_start.elapsed().as_millis() as u64,
            "Transform context preparation complete."
        );

        // --- VRL compilation (parallel via tokio::task::spawn) ---
        // Each transform.build() compiles VRL programs. Using tokio::task::spawn
        // distributes work across all worker threads for true CPU parallelism.
        let vrl_start = Instant::now();
        let mut build_handles = Vec::with_capacity(prepared_transforms.len());

        for (key, inner, context, node, span) in prepared_transforms {
            let key_clone = key.clone();
            let handle = tokio::task::spawn(async move {
                let result = inner
                    .build(&context)
                    .instrument(span.clone())
                    .await;
                (key_clone, result, node, span)
            });
            build_handles.push((key, handle));
        }

        // Collect results and wire up topology
        for (key, handle) in build_handles {
            match handle.await {
                Err(join_error) => {
                    self.errors.push(format!("Transform \"{key}\": task panicked: {join_error}"));
                    continue;
                }
                Ok((_key, result, node, span)) => {
                    match result {
                        Err(error) => {
                            self.errors.push(format!("Transform \"{key}\": {error}"));
                            continue;
                        }
                        Ok(transform) => {
                            let metrics = ChannelMetricMetadata::new(TRANSFORM_CHANNEL_METRIC_PREFIX, None);
                            let (input_tx, input_rx) = TopologyBuilder::standalone_memory(
                                TOPOLOGY_BUFFER_SIZE,
                                WhenFull::Block,
                                &span,
                                Some(metrics),
                                self.config.global.buffer_utilization_ewma_alpha,
                            );

                            self.inputs
                                .insert(key.clone(), (input_tx, node.inputs.clone()));

                            let (transform_task, transform_outputs) =
                                build_transform(transform, node, input_rx, &self.utilization_registry);

                            self.outputs.extend(transform_outputs);
                            self.tasks.insert(key.clone(), transform_task);
                        }
                    }
                }
            }
        }

        info!(
            elapsed_ms = vrl_start.elapsed().as_millis() as u64,
            "VRL compilation and wiring complete."
        );

        info!(
            total_elapsed_ms = build_transforms_start.elapsed().as_millis() as u64,
            "Transform build pipeline finished."
        );
    }

    async fn build_sinks(&mut self, enrichment_tables: &vector_lib::enrichment::TableRegistry) {
        let table_sinks = self
            .config
            .enrichment_tables
            .iter()
            .filter_map(|(key, table)| table.as_sink(key))
            .collect::<Vec<_>>();
        for (key, sink) in self
            .config
            .sinks()
            .filter(|(key, _)| self.diff.sinks.contains_new(key))
            .chain(
                table_sinks
                    .iter()
                    .map(|(key, sink)| (key, sink))
                    .filter(|(key, _)| self.diff.enrichment_tables.contains_new(key)),
            )
        {
            debug!(component_id = %key, "Building new sink.");

            let sink_inputs = &sink.inputs;
            let healthcheck = sink.healthcheck();
            let enable_healthcheck = healthcheck.enabled && self.config.healthchecks.enabled;
            let healthcheck_timeout = healthcheck.timeout;

            let typetag = sink.inner.get_component_name();
            let input_type = sink.inner.input().data_type();

            let span = error_span!(
                "sink",
                component_kind = "sink",
                component_id = %key.id(),
                component_type = %sink.inner.get_component_name(),
            );
            let _entered_span = span.enter();

            // At this point, we've validated that all transforms are valid, including any
            // transform that mutates the schema provided by their sources. We can now validate the
            // schema expectations of each individual sink.
            if let Err(mut err) = schema::validate_sink_expectations(
                key,
                sink,
                self.config,
                enrichment_tables.clone(),
            ) {
                self.errors.append(&mut err);
            };

            let (tx, rx) = match self.buffers.remove(key) {
                Some(buffer) => buffer,
                _ => {
                    let buffer_type =
                        match sink.buffer.stages().first().expect("cant ever be empty") {
                            BufferType::Memory { .. } => "memory",
                            BufferType::DiskV2 { .. } => "disk",
                        };
                    let buffer_span = error_span!("sink", buffer_type);
                    let buffer = sink
                        .buffer
                        .build(
                            self.config.global.data_dir.clone(),
                            key.to_string(),
                            buffer_span,
                        )
                        .await;
                    match buffer {
                        Err(error) => {
                            self.errors.push(format!("Sink \"{key}\": {error}"));
                            continue;
                        }
                        Ok((tx, rx)) => (tx, Arc::new(Mutex::new(Some(rx.into_stream())))),
                    }
                }
            };

            let cx = SinkContext {
                healthcheck,
                globals: self.config.global.clone(),
                enrichment_tables: enrichment_tables.clone(),
                metrics_storage: METRICS_STORAGE.clone(),
                proxy: ProxyConfig::merge_with_env(&self.config.global.proxy, sink.proxy()),
                schema: self.config.schema,
                app_name: crate::get_app_name().to_string(),
                app_name_slug: crate::get_slugified_app_name(),
                extra_context: self.extra_context.clone(),
            };

            let (sink, healthcheck) = match sink.inner.build(cx).await {
                Err(error) => {
                    self.errors.push(format!("Sink \"{key}\": {error}"));
                    continue;
                }
                Ok(built) => built,
            };

            let (trigger, tripwire) = Tripwire::new();

            let utilization_sender = self
                .utilization_registry
                .add_component(key.clone(), gauge!("utilization"));
            let component_key = key.clone();
            let sink = async move {
                debug!("Sink starting.");

                // Why is this Arc<Mutex<Option<_>>> needed you ask.
                // In case when this function build_pieces errors
                // this future won't be run so this rx won't be taken
                // which will enable us to reuse rx to rebuild
                // old configuration by passing this Arc<Mutex<Option<_>>>
                // yet again.
                let rx = rx
                    .lock()
                    .unwrap()
                    .take()
                    .expect("Task started but input has been taken.");

                let mut rx = wrap(utilization_sender, component_key.clone(), rx);

                let events_received = register!(EventsReceived);
                sink.run(
                    rx.by_ref()
                        .filter(|events: &EventArray| ready(filter_events_type(events, input_type)))
                        .inspect(|events| {
                            events_received.emit(CountByteSize(
                                events.len(),
                                events.estimated_json_encoded_size_of(),
                            ))
                        })
                        .take_until_if(tripwire),
                )
                .await
                .map(|_| {
                    debug!("Sink finished normally.");
                    TaskOutput::Sink(rx)
                })
                .map_err(|_| {
                    debug!("Sink finished with an error.");
                    TaskError::Opaque
                })
            };

            let task = Task::new(key.clone(), typetag, sink);

            let component_key = key.clone();
            let healthcheck_task = async move {
                if enable_healthcheck {
                    timeout(healthcheck_timeout, healthcheck)
                        .map(|result| match result {
                            Ok(Ok(_)) => {
                                info!("Healthcheck passed.");
                                Ok(TaskOutput::Healthcheck)
                            }
                            Ok(Err(error)) => {
                                error!(
                                    msg = "Healthcheck failed.",
                                    %error,
                                    component_kind = "sink",
                                    component_type = typetag,
                                    component_id = %component_key.id(),
                                );
                                Err(TaskError::wrapped(error))
                            }
                            Err(e) => {
                                error!(
                                    msg = "Healthcheck timed out.",
                                    component_kind = "sink",
                                    component_type = typetag,
                                    component_id = %component_key.id(),
                                );
                                Err(TaskError::wrapped(Box::new(e)))
                            }
                        })
                        .await
                } else {
                    info!("Healthcheck disabled.");
                    Ok(TaskOutput::Healthcheck)
                }
            };

            let healthcheck_task = Task::new(key.clone(), typetag, healthcheck_task);

            self.inputs.insert(key.clone(), (tx, sink_inputs.clone()));
            self.healthchecks.insert(key.clone(), healthcheck_task);
            self.tasks.insert(key.clone(), task);
            self.detach_triggers.insert(key.clone(), trigger);
        }
    }
}

pub async fn reload_enrichment_tables(config: &Config) {
    let mut enrichment_tables = HashMap::new();
    // Build enrichment tables
    'tables: for (name, table_outer) in config.enrichment_tables.iter() {
        let table_name = name.to_string();
        if ENRICHMENT_TABLES.needs_reload(&table_name) {
            let indexes = Some(ENRICHMENT_TABLES.index_fields(&table_name));

            let mut table = match table_outer.inner.build(&config.global).await {
                Ok(table) => table,
                Err(error) => {
                    error!("Enrichment table \"{name}\" reload failed: {error}");
                    continue;
                }
            };

            if let Some(indexes) = indexes {
                for (case, index) in indexes {
                    match table
                        .add_index(case, &index.iter().map(|s| s.as_ref()).collect::<Vec<_>>())
                    {
                        Ok(_) => (),
                        Err(error) => {
                            // If there is an error adding an index we do not want to use the reloaded
                            // data, the previously loaded data will still need to be used.
                            // Just report the error and continue.
                            error!(
                                message = "Unable to add index to reloaded enrichment table.",
                                table = ?name.to_string(),
                                %error
                            );
                            continue 'tables;
                        }
                    }
                }
            }

            enrichment_tables.insert(table_name, table);
        }
    }

    ENRICHMENT_TABLES.load(enrichment_tables);
    ENRICHMENT_TABLES.finish_load();
}

pub struct TopologyPieces {
    pub(super) inputs: HashMap<ComponentKey, (BufferSender<EventArray>, Inputs<OutputId>)>,
    pub(crate) outputs: HashMap<ComponentKey, HashMap<Option<String>, fanout::ControlChannel>>,
    pub(super) tasks: HashMap<ComponentKey, Task>,
    pub(crate) source_tasks: HashMap<ComponentKey, Task>,
    pub(super) healthchecks: HashMap<ComponentKey, Task>,
    pub(crate) shutdown_coordinator: SourceShutdownCoordinator,
    pub(crate) detach_triggers: HashMap<ComponentKey, Trigger>,
    pub(crate) metrics_storage: MetricsStorage,
    pub(crate) utilization: Option<(UtilizationEmitter, UtilizationRegistry)>,
}

/// Builder for constructing TopologyPieces with a fluent API.
///
/// # Examples
///
/// ```ignore
/// let pieces = TopologyPiecesBuilder::new(&config, &diff)
///     .with_buffers(buffers)
///     .with_extra_context(extra_context)
///     .build()
///     .await?;
/// ```
pub struct TopologyPiecesBuilder<'a> {
    config: &'a Config,
    diff: &'a ConfigDiff,
    buffers: HashMap<ComponentKey, BuiltBuffer>,
    extra_context: ExtraContext,
    utilization_registry: Option<UtilizationRegistry>,
}

impl<'a> TopologyPiecesBuilder<'a> {
    /// Creates a new builder with required parameters.
    pub fn new(config: &'a Config, diff: &'a ConfigDiff) -> Self {
        Self {
            config,
            diff,
            buffers: HashMap::new(),
            extra_context: ExtraContext::default(),
            utilization_registry: None,
        }
    }

    /// Sets the buffers for the topology.
    pub fn with_buffers(mut self, buffers: HashMap<ComponentKey, BuiltBuffer>) -> Self {
        self.buffers = buffers;
        self
    }

    /// Sets the extra context for the topology.
    pub fn with_extra_context(mut self, extra_context: ExtraContext) -> Self {
        self.extra_context = extra_context;
        self
    }

    /// Sets the utilization registry for the topology.
    pub fn with_utilization_registry(mut self, registry: Option<UtilizationRegistry>) -> Self {
        self.utilization_registry = registry;
        self
    }

    /// Builds the topology pieces, returning errors if any occur.
    ///
    /// Use this method when you need to handle errors explicitly,
    /// such as in tests or validation code.
    pub async fn build(self) -> Result<TopologyPieces, Vec<String>> {
        Builder::new(
            self.config,
            self.diff,
            self.buffers,
            self.extra_context,
            self.utilization_registry,
        )
        .build()
        .await
    }

    /// Builds the topology pieces, logging any errors that occur.
    ///
    /// Use this method for runtime configuration loading where
    /// errors should be logged and execution should continue.
    pub async fn build_or_log_errors(self) -> Option<TopologyPieces> {
        match self.build().await {
            Err(errors) => {
                for error in errors {
                    error!(message = "Configuration error.", %error, internal_log_rate_limit = false);
                }
                None
            }
            Ok(new_pieces) => Some(new_pieces),
        }
    }
}

impl TopologyPieces {
    pub async fn build_or_log_errors(
        config: &Config,
        diff: &ConfigDiff,
        buffers: HashMap<ComponentKey, BuiltBuffer>,
        extra_context: ExtraContext,
        utilization_registry: Option<UtilizationRegistry>,
    ) -> Option<Self> {
        TopologyPiecesBuilder::new(config, diff)
            .with_buffers(buffers)
            .with_extra_context(extra_context)
            .with_utilization_registry(utilization_registry)
            .build_or_log_errors()
            .await
    }

    /// Builds only the new pieces, and doesn't check their topology.
    pub async fn build(
        config: &super::Config,
        diff: &ConfigDiff,
        buffers: HashMap<ComponentKey, BuiltBuffer>,
        extra_context: ExtraContext,
        utilization_registry: Option<UtilizationRegistry>,
    ) -> Result<Self, Vec<String>> {
        TopologyPiecesBuilder::new(config, diff)
            .with_buffers(buffers)
            .with_extra_context(extra_context)
            .with_utilization_registry(utilization_registry)
            .build()
            .await
    }
}

const fn filter_events_type(events: &EventArray, data_type: DataType) -> bool {
    match events {
        EventArray::Logs(_) => data_type.contains(DataType::Log),
        EventArray::Metrics(_) => data_type.contains(DataType::Metric),
        EventArray::Traces(_) => data_type.contains(DataType::Trace),
    }
}

#[derive(Debug, Clone)]
struct TransformNode {
    key: ComponentKey,
    typetag: &'static str,
    inputs: Inputs<OutputId>,
    input_details: Input,
    outputs: Vec<TransformOutput>,
    enable_concurrency: bool,
}

impl TransformNode {
    pub fn from_parts(
        key: ComponentKey,
        context: &TransformContext,
        transform: &TransformOuter<OutputId>,
        schema_definition: &[(OutputId, Definition)],
    ) -> Self {
        Self {
            key,
            typetag: transform.inner.get_component_name(),
            inputs: transform.inputs.clone(),
            input_details: transform.inner.input(),
            outputs: transform.inner.outputs(context, schema_definition),
            enable_concurrency: transform.inner.enable_concurrency(),
        }
    }
}

fn build_transform(
    transform: Transform,
    node: TransformNode,
    input_rx: BufferReceiver<EventArray>,
    utilization_registry: &UtilizationRegistry,
) -> (Task, HashMap<OutputId, fanout::ControlChannel>) {
    match transform {
        // TODO: avoid the double boxing for function transforms here
        Transform::Function(t) => {
            build_sync_transform(Box::new(t), node, input_rx, utilization_registry)
        }
        Transform::Synchronous(t) => build_sync_transform(t, node, input_rx, utilization_registry),
        Transform::Task(t) => build_task_transform(
            t,
            input_rx,
            node.input_details.data_type(),
            node.typetag,
            &node.key,
            &node.outputs,
            utilization_registry,
        ),
    }
}

fn build_sync_transform(
    t: Box<dyn SyncTransform>,
    node: TransformNode,
    input_rx: BufferReceiver<EventArray>,
    utilization_registry: &UtilizationRegistry,
) -> (Task, HashMap<OutputId, fanout::ControlChannel>) {
    let (outputs, controls) = TransformOutputs::new(node.outputs, &node.key);

    let sender = utilization_registry.add_component(node.key.clone(), gauge!("utilization"));
    let runner = Runner::new(t, input_rx, sender, node.input_details.data_type(), outputs);
    let transform = if node.enable_concurrency {
        runner.run_concurrently().boxed()
    } else {
        runner.run_inline().boxed()
    };

    let transform = async move {
        debug!("Synchronous transform starting.");

        match transform.await {
            Ok(v) => {
                debug!("Synchronous transform finished normally.");
                Ok(v)
            }
            Err(e) => {
                debug!("Synchronous transform finished with an error.");
                Err(e)
            }
        }
    };

    let mut output_controls = HashMap::new();
    for (name, control) in controls {
        let id = name
            .map(|name| OutputId::from((&node.key, name)))
            .unwrap_or_else(|| OutputId::from(&node.key));
        output_controls.insert(id, control);
    }

    let task = Task::new(node.key.clone(), node.typetag, transform);

    (task, output_controls)
}

struct Runner {
    transform: Box<dyn SyncTransform>,
    input_rx: Option<BufferReceiver<EventArray>>,
    input_type: DataType,
    outputs: TransformOutputs,
    timer_tx: UtilizationComponentSender,
    events_received: Registered<EventsReceived>,
}

impl Runner {
    fn new(
        transform: Box<dyn SyncTransform>,
        input_rx: BufferReceiver<EventArray>,
        timer_tx: UtilizationComponentSender,
        input_type: DataType,
        outputs: TransformOutputs,
    ) -> Self {
        Self {
            transform,
            input_rx: Some(input_rx),
            input_type,
            outputs,
            timer_tx,
            events_received: register!(EventsReceived),
        }
    }

    fn on_events_received(&mut self, events: &EventArray) {
        self.timer_tx.try_send_stop_wait();

        self.events_received.emit(CountByteSize(
            events.len(),
            events.estimated_json_encoded_size_of(),
        ));
    }

    async fn send_outputs(&mut self, outputs_buf: &mut TransformOutputsBuf) -> crate::Result<()> {
        self.timer_tx.try_send_start_wait();
        self.outputs.send(outputs_buf).await
    }

    async fn run_inline(mut self) -> TaskResult {
        // 128 is an arbitrary, smallish constant
        const INLINE_BATCH_SIZE: usize = 128;

        let mut outputs_buf = self.outputs.new_buf_with_capacity(INLINE_BATCH_SIZE);

        let mut input_rx = self
            .input_rx
            .take()
            .expect("can't run runner twice")
            .into_stream()
            .filter(move |events| ready(filter_events_type(events, self.input_type)));

        self.timer_tx.try_send_start_wait();
        while let Some(events) = input_rx.next().await {
            self.on_events_received(&events);
            self.transform.transform_all(events, &mut outputs_buf);
            self.send_outputs(&mut outputs_buf)
                .await
                .map_err(TaskError::wrapped)?;
        }

        Ok(TaskOutput::Transform)
    }

    async fn run_concurrently(mut self) -> TaskResult {
        let input_rx = self
            .input_rx
            .take()
            .expect("can't run runner twice")
            .into_stream()
            .filter(move |events| ready(filter_events_type(events, self.input_type)));

        let mut input_rx =
            super::ready_arrays::ReadyArrays::with_capacity(input_rx, READY_ARRAY_CAPACITY);

        let mut in_flight = FuturesOrdered::new();
        let mut shutting_down = false;

        self.timer_tx.try_send_start_wait();
        loop {
            tokio::select! {
                biased;

                result = in_flight.next(), if !in_flight.is_empty() => {
                    match result {
                        Some(Ok(outputs_buf)) => {
                            let mut outputs_buf: TransformOutputsBuf = outputs_buf;
                            self.send_outputs(&mut outputs_buf).await
                                .map_err(TaskError::wrapped)?;
                        }
                        _ => unreachable!("join error or bad poll"),
                    }
                }

                input_arrays = input_rx.next(), if in_flight.len() < *TRANSFORM_CONCURRENCY_LIMIT && !shutting_down => {
                    match input_arrays {
                        Some(input_arrays) => {
                            let mut len = 0;
                            for events in &input_arrays {
                                self.on_events_received(events);
                                len += events.len();
                            }

                            let mut t = self.transform.clone();
                            let mut outputs_buf = self.outputs.new_buf_with_capacity(len);
                            let task = tokio::spawn(async move {
                                for events in input_arrays {
                                    t.transform_all(events, &mut outputs_buf);
                                }
                                outputs_buf
                            }.in_current_span());
                            in_flight.push_back(task);
                        }
                        None => {
                            shutting_down = true;
                            continue
                        }
                    }
                }

                else => {
                    if shutting_down {
                        break
                    }
                }
            }
        }

        Ok(TaskOutput::Transform)
    }
}

fn build_task_transform(
    t: Box<dyn TaskTransform<EventArray>>,
    input_rx: BufferReceiver<EventArray>,
    input_type: DataType,
    typetag: &str,
    key: &ComponentKey,
    outputs: &[TransformOutput],
    utilization_registry: &UtilizationRegistry,
) -> (Task, HashMap<OutputId, fanout::ControlChannel>) {
    let (mut fanout, control) = Fanout::new();

    let sender = utilization_registry.add_component(key.clone(), gauge!("utilization"));
    let input_rx = wrap(sender, key.clone(), input_rx.into_stream());

    let events_received = register!(EventsReceived);
    let filtered = input_rx
        .filter(move |events| ready(filter_events_type(events, input_type)))
        .inspect(move |events| {
            events_received.emit(CountByteSize(
                events.len(),
                events.estimated_json_encoded_size_of(),
            ))
        });
    let events_sent = register!(EventsSent::from(internal_event::Output(None)));
    let output_id = Arc::new(OutputId {
        component: key.clone(),
        port: None,
    });

    // Task transforms can only write to the default output, so only a single schema def map is needed
    let schema_definition_map = outputs
        .iter()
        .find(|x| x.port.is_none())
        .expect("output for default port required for task transforms")
        .log_schema_definitions
        .clone()
        .into_iter()
        .map(|(key, value)| (key, Arc::new(value)))
        .collect();

    let stream = t
        .transform(Box::pin(filtered))
        .map(move |mut events| {
            for event in events.iter_events_mut() {
                update_runtime_schema_definition(event, &output_id, &schema_definition_map);
            }
            (events, Instant::now())
        })
        .inspect(move |(events, _): &(EventArray, Instant)| {
            events_sent.emit(CountByteSize(
                events.len(),
                events.estimated_json_encoded_size_of(),
            ));
        });
    let transform = async move {
        debug!("Task transform starting.");

        match fanout.send_stream(stream).await {
            Ok(()) => {
                debug!("Task transform finished normally.");
                Ok(TaskOutput::Transform)
            }
            Err(e) => {
                debug!("Task transform finished with an error.");
                Err(TaskError::wrapped(e))
            }
        }
    }
    .boxed();

    let mut outputs = HashMap::new();
    outputs.insert(OutputId::from(key), control);

    let task = Task::new(key.clone(), typetag, transform);

    (task, outputs)
}
