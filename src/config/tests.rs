use super::*;

#[test]
fn missing_exclude_dirs_uses_default_exclusions_and_persists_them() {
    let root = std::env::temp_dir().join(format!("fd-rdd-config-test-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("create temp dir");
    let path = root.join("config.toml");
    std::fs::write(
        &path,
        r#"
roots = ["~"]
http_port = 6060
"#,
    )
    .expect("write config");

    let cfg = Config::load_from_path(&path).expect("config should parse");

    assert!(cfg.exclude_dirs.contains(&"node_modules".to_string()));
    assert!(cfg.exclude_dirs.contains(&"target".to_string()));
    assert!(cfg.exclude_dirs.contains(&".git".to_string()));

    let persisted = std::fs::read_to_string(&path).expect("read persisted config");
    assert!(persisted.contains("exclude_dirs"));
    assert!(persisted.contains("node_modules"));

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn explicit_exclude_dirs_are_normalized_after_load_step() {
    let mut cfg: Config = toml::from_str(
        r#"
roots = ["~"]
exclude_dirs = ["node_modules", "/target/", "", "node_modules"]
"#,
    )
    .expect("config should parse");

    cfg.exclude_dirs = normalize_exclude_dirs(cfg.exclude_dirs);

    assert_eq!(
        cfg.exclude_dirs,
        vec!["node_modules".to_string(), "target".to_string()]
    );
}

#[test]
fn explicit_exclude_dirs_are_not_replaced_by_defaults() {
    let root = std::env::temp_dir().join(format!(
        "fd-rdd-config-explicit-test-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("create temp dir");
    let path = root.join("config.toml");
    std::fs::write(
        &path,
        r#"
roots = ["~"]
exclude_dirs = ["custom_cache"]
"#,
    )
    .expect("write config");

    let cfg = Config::load_from_path(&path).expect("config should parse");

    assert_eq!(cfg.exclude_dirs, vec!["custom_cache".to_string()]);
    let persisted = std::fs::read_to_string(&path).expect("read persisted config");
    assert!(!persisted.contains("node_modules"));

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn tiered_watch_defaults_include_ephemeral_lease_controls() {
    let cfg: Config = toml::from_str(
        r#"
roots = ["~"]
watch_mode = "tiered"

[tiered_watch]
max_watch_dirs = 16
"#,
    )
    .expect("config should parse with partial tiered_watch table");

    assert_eq!(cfg.tiered_watch.max_watch_dirs, 16);
    assert_eq!(
        cfg.tiered_watch.l0_max_cost_per_root,
        DEFAULT_TIERED_L0_MAX_COST_PER_ROOT
    );
    assert_eq!(cfg.tiered_watch.ephemeral_watch_budget, 256);
    assert_eq!(cfg.tiered_watch.ephemeral_watch_ttl_secs, 600);
    assert_eq!(cfg.tiered_watch.ephemeral_idle_secs, 120);
    assert_eq!(cfg.tiered_watch.ephemeral_max_cost_per_root, 64);
    assert!(cfg.tiered_watch.l1_l2_fast_scan_enabled);
    assert_eq!(cfg.tiered_watch.l1_l2_fast_scan_target_secs, 5);
    assert_eq!(cfg.tiered_watch.l1_l2_fast_scan_tick_ms, 1_000);
    assert_eq!(
        cfg.tiered_watch.network_fast_scan_mode,
        NetworkFastScanMode::BestEffort
    );
    assert!(cfg
        .tiered_watch
        .project_markers
        .contains(&"Cargo.toml".to_string()));
    assert_eq!(cfg.tiered_watch.profile, TieredWatchProfile::Balanced);
    assert_eq!(cfg.tiered_watch.l3_scan_policy, L3ScanPolicy::Interval);
    assert_eq!(
        cfg.tiered_watch.l3_scan_interval_secs,
        DEFAULT_L3_SCAN_INTERVAL_SECS
    );

    let toml = toml::to_string_pretty(&Config::default()).expect("serialize default config");
    assert!(toml.contains("l3_scan_policy"));
    assert!(toml.contains("profile"));
    assert!(toml.contains("l0_max_cost_per_root"));
    assert!(toml.contains("strict_required_hot_dirs"));
    assert!(toml.contains("strict_fail_on_budget_exceeded"));
    assert!(toml.contains("l3_scan_interval_secs"));
    assert!(toml.contains("ephemeral_watch_budget"));
    assert!(toml.contains("ephemeral_watch_ttl_secs"));
    assert!(toml.contains("ephemeral_idle_secs"));
    assert!(toml.contains("ephemeral_max_cost_per_root"));
    assert!(toml.contains("l1_l2_fast_scan_enabled"));
    assert!(toml.contains("l1_l2_fast_scan_target_secs"));
    assert!(toml.contains("network_fast_scan_mode"));
    assert!(toml.contains("project_markers"));
}

#[test]
fn tiered_watch_fast_scan_overrides_parse() {
    let cfg: Config = toml::from_str(
        r#"
roots = ["~"]
watch_mode = "tiered"

[tiered_watch]
l1_l2_fast_scan_enabled = false
l1_l2_fast_scan_target_secs = 7
l1_l2_fast_scan_tick_ms = 250
l1_l2_fast_scan_stat_budget_per_tick = 111
l1_l2_fast_scan_readdir_budget_per_tick = 22
l1_l2_fast_scan_bootstrap_budget_per_tick = 333
l1_l2_fast_scan_hotset_max_leases = 77
l1_l2_fast_scan_lease_ttl_secs = 88
l1_l2_fast_scan_proc_sampler_lease_ttl_secs = 99
l1_l2_fast_scan_explicit_lease_ttl_secs = 11
l1_l2_fast_scan_sentinel_registry_max_entries = 55
network_fast_scan_mode = "strict_poll"
network_fast_scan_stat_budget_per_tick = 44
network_fast_scan_readdir_budget_per_tick = 5
"#,
    )
    .expect("fast scan config should parse");

    assert!(!cfg.tiered_watch.l1_l2_fast_scan_enabled);
    assert_eq!(cfg.tiered_watch.l1_l2_fast_scan_target_secs, 7);
    assert_eq!(cfg.tiered_watch.l1_l2_fast_scan_tick_ms, 250);
    assert_eq!(cfg.tiered_watch.l1_l2_fast_scan_stat_budget_per_tick, 111);
    assert_eq!(cfg.tiered_watch.l1_l2_fast_scan_readdir_budget_per_tick, 22);
    assert_eq!(
        cfg.tiered_watch.l1_l2_fast_scan_bootstrap_budget_per_tick,
        333
    );
    assert_eq!(cfg.tiered_watch.l1_l2_fast_scan_hotset_max_leases, 77);
    assert_eq!(cfg.tiered_watch.l1_l2_fast_scan_lease_ttl_secs, 88);
    assert_eq!(
        cfg.tiered_watch.l1_l2_fast_scan_proc_sampler_lease_ttl_secs,
        99
    );
    assert_eq!(cfg.tiered_watch.l1_l2_fast_scan_explicit_lease_ttl_secs, 11);
    assert_eq!(
        cfg.tiered_watch
            .l1_l2_fast_scan_sentinel_registry_max_entries,
        55
    );
    assert_eq!(
        cfg.tiered_watch.network_fast_scan_mode,
        NetworkFastScanMode::StrictPoll
    );
    assert_eq!(cfg.tiered_watch.network_fast_scan_stat_budget_per_tick, 44);
    assert_eq!(
        cfg.tiered_watch.network_fast_scan_readdir_budget_per_tick,
        5
    );
}

#[test]
fn runtime_profile_defaults_and_accepts_memory_light() {
    let default_cfg: Config = toml::from_str(
        r#"
roots = ["~"]
"#,
    )
    .expect("config should parse without runtime profile");

    assert_eq!(default_cfg.runtime_profile, RuntimeProfile::Default);

    let memory_light: Config = toml::from_str(
        r#"
roots = ["~"]
runtime_profile = "memory_light"
"#,
    )
    .expect("memory_light runtime profile should parse");

    assert_eq!(memory_light.runtime_profile, RuntimeProfile::MemoryLight);
    assert_eq!(RuntimeProfile::MemoryLight.as_str(), "memory_light");

    let toml = toml::to_string_pretty(&Config::default()).expect("serialize default config");
    assert!(toml.contains("runtime_profile"));
}

#[test]
fn query_verify_defaults_and_overrides_parse() {
    let default_cfg: Config = toml::from_str(
        r#"
roots = ["~"]
"#,
    )
    .expect("config should parse without query table");

    assert_eq!(default_cfg.query.max_verify_per_query, 150);
    assert_eq!(default_cfg.query.verify_timeout_ms, 75);
    assert!(!default_cfg.lazy_validation_enabled);

    let cfg: Config = toml::from_str(
        r#"
roots = ["~"]

[query]
max_verify_per_query = 123
verify_timeout_ms = 60
"#,
    )
    .expect("query config should parse");

    assert_eq!(cfg.query.max_verify_per_query, 123);
    assert_eq!(cfg.query.verify_timeout_ms, 60);

    // Old config files with allow_sync_readdir should still parse (field is ignored).
    let _legacy: Config = toml::from_str(
        r#"
roots = ["~"]

[query]
allow_sync_readdir = true
"#,
    )
    .expect("legacy config with allow_sync_readdir should still parse");

    let toml = toml::to_string_pretty(&Config::default()).expect("serialize default config");
    assert!(toml.contains("[query]"));
    assert!(toml.contains("max_verify_per_query"));
}

#[test]
fn proc_sampler_defaults_and_overrides_parse() {
    let default_cfg: Config = toml::from_str(
        r#"
roots = ["~"]
"#,
    )
    .expect("config should parse without proc_sampler table");

    assert!(default_cfg.proc_sampler.enabled);
    assert_eq!(default_cfg.proc_sampler.interval_ms, 1_000);
    assert_eq!(default_cfg.proc_sampler.max_pids_per_tick, 128);
    assert_eq!(default_cfg.proc_sampler.max_fds_per_pid, 64);
    assert_eq!(default_cfg.proc_sampler.max_dirs_per_tick, 32);

    let cfg: Config = toml::from_str(
        r#"
roots = ["~"]

[proc_sampler]
enabled = false
interval_ms = 250
max_pids_per_tick = 7
max_fds_per_pid = 8
max_dirs_per_tick = 9
"#,
    )
    .expect("proc sampler config should parse");

    assert!(!cfg.proc_sampler.enabled);
    assert_eq!(cfg.proc_sampler.interval_ms, 250);
    assert_eq!(cfg.proc_sampler.max_pids_per_tick, 7);
    assert_eq!(cfg.proc_sampler.max_fds_per_pid, 8);
    assert_eq!(cfg.proc_sampler.max_dirs_per_tick, 9);

    let toml = toml::to_string_pretty(&Config::default()).expect("serialize default config");
    assert!(toml.contains("[proc_sampler]"));
    assert!(toml.contains("max_pids_per_tick"));
    assert!(toml.contains("max_dirs_per_tick"));
}

#[test]
fn content_index_defaults_disabled_and_accepts_policy() {
    let default_cfg: Config = toml::from_str(
        r#"
roots = ["~"]
"#,
    )
    .expect("config should parse without content_index table");

    assert!(!default_cfg.content_index.enable);
    assert_eq!(default_cfg.content_index.max_file_size, 1024 * 1024);
    assert!(default_cfg.content_index.include_ext.is_empty());
    assert!(default_cfg.content_index.exclude_ext.is_empty());

    let cfg: Config = toml::from_str(
        r#"
roots = ["~"]

[content_index]
enable = true
max_file_size = 4096
include_ext = ["txt", "md"]
exclude_ext = ["log"]
"#,
    )
    .expect("content_index config should parse");

    assert!(cfg.content_index.enable);
    assert_eq!(cfg.content_index.max_file_size, 4096);
    assert_eq!(cfg.content_index.include_ext, vec!["txt", "md"]);
    assert_eq!(cfg.content_index.exclude_ext, vec!["log"]);

    let toml = toml::to_string_pretty(&Config::default()).expect("serialize default config");
    assert!(toml.contains("[content_index]"));
    assert!(toml.contains("max_file_size"));
}

#[test]
fn tiered_watch_profile_defaults_to_high_watch_budget() {
    let balanced: Config = toml::from_str(
        r#"
roots = ["~"]
watch_mode = "tiered"

[tiered_watch]
"#,
    )
    .expect("balanced profile should parse");

    assert_eq!(balanced.tiered_watch.profile, TieredWatchProfile::Balanced);
    assert_eq!(
        balanced.tiered_watch.max_watch_dirs,
        DEFAULT_TIERED_MAX_WATCH_DIRS
    );

    let cfg: Config = toml::from_str(
        r#"
roots = ["~"]
watch_mode = "tiered"

[tiered_watch]
profile = "strict"
"#,
    )
    .expect("strict profile should parse");

    assert_eq!(cfg.tiered_watch.profile, TieredWatchProfile::Strict);
    assert_eq!(
        cfg.tiered_watch.max_watch_dirs,
        DEFAULT_TIERED_MAX_WATCH_DIRS
    );
    assert!(cfg.tiered_watch.strict_fail_on_budget_exceeded);
    assert_eq!(
        cfg.tiered_watch.strict_required_hot_dirs,
        super::default_hot_dirs()
    );
    assert_eq!(
        cfg.tiered_watch.project_markers,
        super::default_project_markers()
    );
}

#[test]
fn strict_tiered_watch_profile_accepts_explicit_overrides() {
    let cfg: Config = toml::from_str(
        r#"
roots = ["~"]
watch_mode = "tiered"

[tiered_watch]
profile = "strict"
max_watch_dirs = 32
l0_max_cost_per_root = 8
strict_required_hot_dirs = ["~/Documents"]
strict_fail_on_budget_exceeded = false
project_markers = [".git", "WORKSPACE"]
"#,
    )
    .expect("strict overrides should parse");

    assert_eq!(cfg.tiered_watch.profile, TieredWatchProfile::Strict);
    assert_eq!(cfg.tiered_watch.max_watch_dirs, 32);
    assert_eq!(cfg.tiered_watch.l0_max_cost_per_root, 8);
    assert_eq!(
        cfg.tiered_watch.strict_required_hot_dirs,
        vec![PathBuf::from("~/Documents")]
    );
    assert!(!cfg.tiered_watch.strict_fail_on_budget_exceeded);
    assert_eq!(cfg.tiered_watch.project_markers, vec![".git", "WORKSPACE"]);
}

#[test]
fn tiered_watch_l3_scan_policy_accepts_documented_values() {
    let cfg: Config = toml::from_str(
        r#"
roots = ["~"]
watch_mode = "tiered"

[tiered_watch]
l3_scan_policy = "validate_on_query"
l3_scan_interval_secs = 999
"#,
    )
    .expect("config should parse documented L3 policy");

    assert_eq!(
        cfg.tiered_watch.l3_scan_policy,
        L3ScanPolicy::ValidateOnQuery
    );
    assert_eq!(cfg.tiered_watch.l3_scan_interval_secs, 999);

    let legacy_hyphen: TieredWatchConfig = toml::from_str(
        r#"
l3_scan_policy = "validate-on-query"
"#,
    )
    .expect("hyphenated value should remain accepted for CLI-style configs");

    assert_eq!(legacy_hyphen.l3_scan_policy, L3ScanPolicy::ValidateOnQuery);
}

#[test]
fn load_accepts_structured_roots_and_keeps_runtime_state_out_of_config() {
    let root = std::env::temp_dir().join(format!(
        "fd-rdd-config-structured-roots-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("create temp dir");
    let path = root.join("config.toml");
    std::fs::write(
        &path,
        r#"
exclude_dirs = ["target"]

[[roots]]
path = "/mnt/samba"
case_policy = "Auto"
allow_remote = false
one_file_system = true
detected_policy = "Insensitive"
conflict_count = 14
"#,
    )
    .expect("write config");

    let cfg = Config::load_from_path(&path).expect("structured roots should parse");

    assert_eq!(cfg.roots, vec![PathBuf::from("/mnt/samba")]);
    assert_eq!(cfg.root_configs.len(), 1);
    assert_eq!(cfg.root_configs[0].case_policy, RootCasePolicy::Auto);
    assert!(!cfg.root_configs[0].allow_remote);
    assert!(cfg.root_configs[0].one_file_system);

    let toml = cfg.to_toml_string().expect("serialize config");
    assert!(toml.contains("[[roots]]"));
    assert!(toml.contains("case_policy = \"Auto\""));
    assert!(!toml.contains("detected_policy"));
    assert!(!toml.contains("conflict_count"));

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn load_accepts_legacy_roots_array() {
    let root =
        std::env::temp_dir().join(format!("fd-rdd-config-legacy-roots-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("create temp dir");
    let path = root.join("config.toml");
    std::fs::write(
        &path,
        r#"
roots = ["/tmp"]
exclude_dirs = ["target"]
"#,
    )
    .expect("write config");

    let cfg = Config::load_from_path(&path).expect("legacy roots should parse");

    assert_eq!(cfg.roots, vec![PathBuf::from("/tmp")]);
    assert_eq!(cfg.root_configs[0].path, PathBuf::from("/tmp"));
    assert_eq!(cfg.root_configs[0].case_policy, RootCasePolicy::Auto);

    let _ = std::fs::remove_dir_all(root);
}
