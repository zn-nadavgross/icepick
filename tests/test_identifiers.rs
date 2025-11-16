use icepick::spec::NamespaceIdent;

#[test]
fn test_namespace_ident_single_level() {
    let ns = NamespaceIdent::new(vec!["default".to_string()]);
    assert_eq!(ns.as_ref(), &["default"]);
    assert_eq!(ns.to_string(), "default");
}

#[test]
fn test_namespace_ident_multi_level() {
    let ns = NamespaceIdent::new(vec!["catalog".to_string(), "db".to_string()]);
    assert_eq!(ns.as_ref(), &["catalog", "db"]);
    assert_eq!(ns.to_string(), "catalog.db");
}

#[test]
fn test_namespace_ident_empty_fails() {
    let result = std::panic::catch_unwind(|| {
        NamespaceIdent::new(vec![]);
    });
    assert!(result.is_err());
}
