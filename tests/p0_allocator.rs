#[test]
fn allocator_kind_matches_features() {
    let expected = if cfg!(feature = "mimalloc") {
        "mimalloc"
    } else {
        "system"
    };

    assert_eq!(fd_rdd::ALLOCATOR_KIND, expected);
}
