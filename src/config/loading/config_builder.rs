use std::{collections::HashMap, io::Read, path::Path, time::Instant};

use indexmap::IndexMap;
use rayon::prelude::*;
use serde_toml_merge::merge_into_table;
use toml::value::{Table, Value};

use super::{
    ComponentHint, Format, Process, deserialize_table, loader, prepare_input, process_config_file,
    read_dir, secret,
};
use crate::config::{
    ComponentKey, ConfigBuilder, ConfigPath, EnrichmentTableOuter, SinkOuter, SourceOuter,
    TestDefinition, TransformOuter,
};

#[derive(Debug)]
pub struct ConfigBuilderLoader {
    builder: ConfigBuilder,
    secrets: HashMap<String, String>,
    interpolate_env: bool,
}

impl ConfigBuilderLoader {
    /// Sets whether to interpolate environment variables in the config.
    pub const fn interpolate_env(mut self, interpolate: bool) -> Self {
        self.interpolate_env = interpolate;
        self
    }

    /// Sets the secrets map for secret interpolation.
    pub fn secrets(mut self, secrets: HashMap<String, String>) -> Self {
        self.secrets = secrets;
        self
    }

    /// Sets whether to allow empty configuration.
    pub const fn allow_empty(mut self, allow_empty: bool) -> Self {
        self.builder.allow_empty = allow_empty;
        self
    }

    /// Builds the ConfigBuilderLoader and loads configuration from the specified paths.
    /// Files are processed in parallel for improved cold start performance.
    pub fn load_from_paths(
        mut self,
        config_paths: &[ConfigPath],
    ) -> Result<ConfigBuilder, Vec<String>> {
        // Separate files and directories
        let (files, dirs): (Vec<_>, Vec<_>) = config_paths.iter().partition(|p| {
            matches!(p, ConfigPath::File(_, _))
        });

        // Extract file info for parallel processing
        let file_info: Vec<_> = files
            .iter()
            .filter_map(|p| {
                if let ConfigPath::File(path, format_hint) = p {
                    let format = format_hint
                        .or_else(|| Format::from_path(path).ok())
                        .unwrap_or_default();
                    Some((path.clone(), format))
                } else {
                    None
                }
            })
            .collect();

        // Process files in parallel
        let file_read_start = Instant::now();
        let parallel_results: Vec<_> = file_info
            .par_iter()
            .map(|(path, format)| {
                process_config_file(path, *format, self.interpolate_env, &self.secrets)
            })
            .collect();
        info!(
            elapsed_ms = file_read_start.elapsed().as_millis() as u64,
            file_count = file_info.len(),
            "File reading and TOML parsing complete."
        );

        // Collect errors and successful results
        let mut errors = Vec::new();
        let mut tables = Vec::new();

        for result in parallel_results {
            match result {
                Ok(Some((name, table))) => tables.push((name, table)),
                Ok(None) => {}
                Err(errs) => errors.extend(errs),
            }
        }

        // Deserialize tables in parallel for better performance with large configs
        let deser_start = Instant::now();
        let deserialized: Vec<_> = tables
            .into_par_iter()
            .map(|(_name, table)| deserialize_table::<ConfigBuilder>(table))
            .collect();
        info!(
            elapsed_ms = deser_start.elapsed().as_millis() as u64,
            "TOML table deserialization complete."
        );

        // Merge deserialized ConfigBuilders sequentially (order matters)
        let merge_start = Instant::now();
        for result in deserialized {
            match result {
                Ok(config) => {
                    if let Err(errs) = self.builder.append(config) {
                        errors.extend(errs);
                    }
                }
                Err(errs) => errors.extend(errs),
            }
        }
        info!(
            elapsed_ms = merge_start.elapsed().as_millis() as u64,
            "ConfigBuilder merge complete."
        );

        // Process directories with parallel file loading
        let dir_start = Instant::now();
        let dir_count = dirs.len();
        for dir in &dirs {
            if let ConfigPath::Dir(path) = dir {
                if let Err(errs) = self.load_from_dir_parallel(path) {
                    errors.extend(errs);
                }
            }
        }
        if dir_count > 0 {
            info!(
                elapsed_ms = dir_start.elapsed().as_millis() as u64,
                dir_count = dirs.len(),
                "Directory config loading complete."
            );
        }

        if errors.is_empty() {
            Ok(self.builder)
        } else {
            Err(errors)
        }
    }

    /// Loads configuration from a directory with parallel file processing.
    fn load_from_dir_parallel(&mut self, path: &Path) -> Result<(), Vec<String>> {
        let hints = [
            ComponentHint::Source,
            ComponentHint::Transform,
            ComponentHint::Sink,
            ComponentHint::Test,
            ComponentHint::EnrichmentTable,
        ];

        // Collect all files from the root directory
        let root_files = self.collect_files_in_dir(path, false)?;

        // Process root files in parallel
        let root_results: Vec<_> = root_files
            .par_iter()
            .map(|(file_path, format)| {
                process_config_file(file_path, *format, self.interpolate_env, &self.secrets)
            })
            .collect();

        // Merge root files into a combined table
        let mut errors = Vec::new();
        let mut root = Table::new();

        for result in root_results {
            match result {
                Ok(Some((_, table))) => {
                    if let Err(e) = merge_into_table(&mut root, table) {
                        errors.push(e.to_string());
                    }
                }
                Ok(None) => {}
                Err(errs) => errors.extend(errs),
            }
        }

        // Merge the root config first
        if let Err(errs) = self.merge(root, None) {
            errors.extend(errs);
        }

        // Process each component subdirectory
        for hint in hints {
            let component_path = hint.join_path(path);
            if component_path.exists() && component_path.is_dir() {
                // Transforms can be nested, others are flat
                let recurse = matches!(hint, ComponentHint::Transform);
                let component_files = self.collect_files_in_dir(&component_path, recurse)?;

                // Process files in parallel
                let component_results: Vec<_> = component_files
                    .par_iter()
                    .map(|(file_path, format)| {
                        process_config_file(file_path, *format, self.interpolate_env, &self.secrets)
                    })
                    .collect();

                // Merge into a component table
                let mut component_table = Table::new();
                for result in component_results {
                    match result {
                        Ok(Some((name, table))) => {
                            if component_table.contains_key(&name) {
                                // Merge with existing
                                if let Some(Value::Table(existing)) = component_table.get_mut(&name)
                                {
                                    if let Err(e) = merge_into_table(existing, table) {
                                        errors.push(e.to_string());
                                    }
                                }
                            } else {
                                component_table.insert(name, Value::Table(table));
                            }
                        }
                        Ok(None) => {}
                        Err(errs) => errors.extend(errs),
                    }
                }

                // Merge component table with hint
                if let Err(errs) = self.merge(component_table, Some(hint)) {
                    errors.extend(errs);
                }
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Collects all config files in a directory, optionally recursing into subdirectories.
    fn collect_files_in_dir(
        &self,
        path: &Path,
        recurse: bool,
    ) -> Result<Vec<(std::path::PathBuf, Format)>, Vec<String>> {
        let mut files = Vec::new();
        self.collect_files_recursive(path, recurse, &mut files)?;
        Ok(files)
    }

    /// Helper to recursively collect files from directories.
    fn collect_files_recursive(
        &self,
        path: &Path,
        recurse: bool,
        files: &mut Vec<(std::path::PathBuf, Format)>,
    ) -> Result<(), Vec<String>> {
        let readdir = read_dir(path)?;
        let mut errors = Vec::new();

        for entry in readdir {
            match entry {
                Ok(item) => {
                    let entry_path = item.path();
                    if entry_path.is_file() {
                        // Only include files with known formats
                        if let Ok(format) = Format::from_path(&entry_path) {
                            files.push((entry_path, format));
                        }
                    } else if entry_path.is_dir() && recurse {
                        // Skip hidden directories
                        if !entry_path
                            .file_name()
                            .and_then(|name| name.to_str())
                            .map(|name| name.starts_with('.'))
                            .unwrap_or(false)
                        {
                            if let Err(errs) =
                                self.collect_files_recursive(&entry_path, true, files)
                            {
                                errors.extend(errs);
                            }
                        }
                    }
                }
                Err(err) => {
                    errors.push(format!(
                        "Could not read entry in config dir: {:?}, {}.",
                        path, err
                    ));
                }
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Builds the ConfigBuilderLoader and loads configuration from an input reader.
    pub fn load_from_input<R: Read>(
        self,
        input: R,
        format: super::Format,
    ) -> Result<ConfigBuilder, Vec<String>> {
        super::loader_from_input(self, input, format)
    }
}

impl Default for ConfigBuilderLoader {
    /// Creates a new builder with default settings.
    /// By default, environment variable interpolation is enabled.
    fn default() -> Self {
        Self {
            builder: ConfigBuilder::default(),
            secrets: HashMap::new(),
            interpolate_env: true,
        }
    }
}

impl Process for ConfigBuilderLoader {
    /// Prepares input for a `ConfigBuilder` by interpolating environment variables.
    fn prepare<R: Read>(&mut self, input: R) -> Result<String, Vec<String>> {
        let prepared_input = prepare_input(input, self.interpolate_env)?;
        Ok(if self.secrets.is_empty() {
            prepared_input
        } else {
            secret::interpolate(&prepared_input, &self.secrets)?
        })
    }

    /// Merge a TOML `Table` with a `ConfigBuilder`. Component types extend specific keys.
    fn merge(&mut self, table: Table, hint: Option<ComponentHint>) -> Result<(), Vec<String>> {
        match hint {
            Some(ComponentHint::Source) => {
                self.builder.sources.extend(deserialize_table::<
                    IndexMap<ComponentKey, SourceOuter>,
                >(table)?);
            }
            Some(ComponentHint::Sink) => {
                self.builder.sinks.extend(
                    deserialize_table::<IndexMap<ComponentKey, SinkOuter<_>>>(table)?,
                );
            }
            Some(ComponentHint::Transform) => {
                self.builder.transforms.extend(deserialize_table::<
                    IndexMap<ComponentKey, TransformOuter<_>>,
                >(table)?);
            }
            Some(ComponentHint::EnrichmentTable) => {
                self.builder.enrichment_tables.extend(deserialize_table::<
                    IndexMap<ComponentKey, EnrichmentTableOuter<_>>,
                >(table)?);
            }
            Some(ComponentHint::Test) => {
                // This serializes to a `Vec<TestDefinition<_>>`, so we need to first expand
                // it to an ordered map, and then pull out the value, ignoring the keys.
                self.builder.tests.extend(
                    deserialize_table::<IndexMap<String, TestDefinition<String>>>(table)?
                        .into_iter()
                        .map(|(_, test)| test),
                );
            }
            None => {
                self.builder.append(deserialize_table(table)?)?;
            }
        };

        Ok(())
    }
}

impl loader::Loader<ConfigBuilder> for ConfigBuilderLoader {
    /// Returns the resulting `ConfigBuilder`.
    fn take(self) -> ConfigBuilder {
        self.builder
    }
}

#[cfg(all(
    test,
    feature = "sinks-elasticsearch",
    feature = "transforms-sample",
    feature = "sources-demo_logs",
    feature = "sinks-console"
))]
mod tests {
    use std::path::PathBuf;

    use super::ConfigBuilderLoader;
    use crate::config::{ComponentKey, ConfigPath};

    #[test]
    fn load_namespacing_folder() {
        let path = PathBuf::from(".")
            .join("tests")
            .join("namespacing")
            .join("success");
        let configs = vec![ConfigPath::Dir(path)];
        let builder = ConfigBuilderLoader::default()
            .interpolate_env(true)
            .load_from_paths(&configs)
            .unwrap();
        assert!(
            builder
                .transforms
                .contains_key(&ComponentKey::from("apache_parser"))
        );
        assert!(
            builder
                .sources
                .contains_key(&ComponentKey::from("apache_logs"))
        );
        assert!(
            builder
                .sinks
                .contains_key(&ComponentKey::from("es_cluster"))
        );
        assert_eq!(builder.tests.len(), 2);
    }

    #[test]
    fn load_namespacing_ignore_invalid() {
        let path = PathBuf::from(".")
            .join("tests")
            .join("namespacing")
            .join("ignore-invalid");
        let configs = vec![ConfigPath::Dir(path)];
        ConfigBuilderLoader::default()
            .interpolate_env(true)
            .load_from_paths(&configs)
            .unwrap();
    }

    #[test]
    fn load_directory_ignores_unknown_file_formats() {
        let path = PathBuf::from(".")
            .join("tests")
            .join("config-dir")
            .join("ignore-unknown");
        let configs = vec![ConfigPath::Dir(path)];
        ConfigBuilderLoader::default()
            .interpolate_env(true)
            .load_from_paths(&configs)
            .unwrap();
    }

    #[test]
    fn load_directory_globals() {
        let path = PathBuf::from(".")
            .join("tests")
            .join("config-dir")
            .join("globals");
        let configs = vec![ConfigPath::Dir(path)];
        ConfigBuilderLoader::default()
            .interpolate_env(true)
            .load_from_paths(&configs)
            .unwrap();
    }

    #[test]
    fn load_directory_globals_duplicates() {
        let path = PathBuf::from(".")
            .join("tests")
            .join("config-dir")
            .join("globals-duplicate");
        let configs = vec![ConfigPath::Dir(path)];
        ConfigBuilderLoader::default()
            .interpolate_env(true)
            .load_from_paths(&configs)
            .unwrap();
    }
}

#[cfg(test)]
mod parallel_loading_tests {
    use tempfile::TempDir;

    use super::*;
    use crate::config::Format;

    /// Test that parallel loading of multiple individual config files works correctly.
    #[test]
    fn test_parallel_load_multiple_files() {
        let temp_dir = TempDir::new().unwrap();

        // Create multiple config files with different sources
        let config1 = r#"
[sources.source1]
type = "demo_logs"
format = "json"
"#;
        let config2 = r#"
[sources.source2]
type = "demo_logs"
format = "json"
"#;
        let config3 = r#"
[sources.source3]
type = "demo_logs"
format = "json"
"#;

        let file1 = temp_dir.path().join("config1.toml");
        let file2 = temp_dir.path().join("config2.toml");
        let file3 = temp_dir.path().join("config3.toml");

        std::fs::write(&file1, config1).unwrap();
        std::fs::write(&file2, config2).unwrap();
        std::fs::write(&file3, config3).unwrap();

        let configs = vec![
            ConfigPath::File(file1, Some(Format::Toml)),
            ConfigPath::File(file2, Some(Format::Toml)),
            ConfigPath::File(file3, Some(Format::Toml)),
        ];

        let builder = ConfigBuilderLoader::default()
            .interpolate_env(false)
            .load_from_paths(&configs)
            .unwrap();

        // All three sources should be loaded
        assert_eq!(builder.sources.len(), 3);
        assert!(builder.sources.contains_key(&ComponentKey::from("source1")));
        assert!(builder.sources.contains_key(&ComponentKey::from("source2")));
        assert!(builder.sources.contains_key(&ComponentKey::from("source3")));
    }

    /// Test that parallel directory loading works correctly.
    #[test]
    fn test_parallel_load_directory_with_components() {
        let temp_dir = TempDir::new().unwrap();

        // Create sources directory
        let sources_dir = temp_dir.path().join("sources");
        std::fs::create_dir(&sources_dir).unwrap();

        // Create multiple source config files
        let source1 = r#"
type = "demo_logs"
format = "json"
"#;
        let source2 = r#"
type = "demo_logs"
format = "json"
"#;

        std::fs::write(sources_dir.join("src1.toml"), source1).unwrap();
        std::fs::write(sources_dir.join("src2.toml"), source2).unwrap();

        // Create sinks directory
        let sinks_dir = temp_dir.path().join("sinks");
        std::fs::create_dir(&sinks_dir).unwrap();

        let sink1 = r#"
type = "console"
inputs = ["src1"]
encoding.codec = "json"
"#;
        std::fs::write(sinks_dir.join("sink1.toml"), sink1).unwrap();

        let configs = vec![ConfigPath::Dir(temp_dir.path().to_path_buf())];

        let builder = ConfigBuilderLoader::default()
            .interpolate_env(false)
            .load_from_paths(&configs)
            .unwrap();

        // Check that sources and sinks are loaded
        assert_eq!(builder.sources.len(), 2);
        assert!(builder.sources.contains_key(&ComponentKey::from("src1")));
        assert!(builder.sources.contains_key(&ComponentKey::from("src2")));
        assert_eq!(builder.sinks.len(), 1);
        assert!(builder.sinks.contains_key(&ComponentKey::from("sink1")));
    }

    /// Test that env var interpolation works with parallel loading.
    #[test]
    fn test_parallel_load_with_env_interpolation() {
        let temp_dir = TempDir::new().unwrap();

        // Set an env var for testing
        // SAFETY: This is a test and we're modifying env vars in a controlled manner
        unsafe {
            std::env::set_var("TEST_PARALLEL_FORMAT", "json");
        }

        let config = r#"
[sources.env_source]
type = "demo_logs"
format = "${TEST_PARALLEL_FORMAT}"
"#;

        let file = temp_dir.path().join("config.toml");
        std::fs::write(&file, config).unwrap();

        let configs = vec![ConfigPath::File(file, Some(Format::Toml))];

        let builder = ConfigBuilderLoader::default()
            .interpolate_env(true)
            .load_from_paths(&configs)
            .unwrap();

        assert!(
            builder
                .sources
                .contains_key(&ComponentKey::from("env_source"))
        );

        // Clean up
        // SAFETY: This is a test cleanup
        unsafe {
            std::env::remove_var("TEST_PARALLEL_FORMAT");
        }
    }

    /// Test that mixed files and directories are processed correctly.
    #[test]
    fn test_parallel_load_mixed_files_and_dirs() {
        let temp_dir = TempDir::new().unwrap();

        // Create a standalone file
        let standalone_config = r#"
[sources.standalone]
type = "demo_logs"
format = "json"
"#;
        let standalone_file = temp_dir.path().join("standalone.toml");
        std::fs::write(&standalone_file, standalone_config).unwrap();

        // Create a subdirectory with components
        let subdir = temp_dir.path().join("subdir");
        std::fs::create_dir(&subdir).unwrap();

        let sources_dir = subdir.join("sources");
        std::fs::create_dir(&sources_dir).unwrap();

        let source_in_dir = r#"
type = "demo_logs"
format = "json"
"#;
        std::fs::write(sources_dir.join("dir_source.toml"), source_in_dir).unwrap();

        let configs = vec![
            ConfigPath::File(standalone_file, Some(Format::Toml)),
            ConfigPath::Dir(subdir),
        ];

        let builder = ConfigBuilderLoader::default()
            .interpolate_env(false)
            .load_from_paths(&configs)
            .unwrap();

        // Both sources should be loaded
        assert_eq!(builder.sources.len(), 2);
        assert!(
            builder
                .sources
                .contains_key(&ComponentKey::from("standalone"))
        );
        assert!(
            builder
                .sources
                .contains_key(&ComponentKey::from("dir_source"))
        );
    }
}
