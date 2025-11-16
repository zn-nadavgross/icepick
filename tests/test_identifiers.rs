use icepick::spec::{NamespaceIdent, TableIdent};

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

#[test]
fn test_table_ident_creation() {
    let ns = NamespaceIdent::new(vec!["default".to_string()]);
    let table = TableIdent::new(ns.clone(), "users".to_string());

    assert_eq!(table.namespace(), &ns);
    assert_eq!(table.name(), "users");
    assert_eq!(table.to_string(), "default.users");
}

#[test]
fn test_table_ident_multi_level_namespace() {
    let ns = NamespaceIdent::new(vec!["catalog".to_string(), "db".to_string()]);
    let table = TableIdent::new(ns.clone(), "events".to_string());

    assert_eq!(table.to_string(), "catalog.db.events");
}

#[test]
fn test_table_ident_from_strs() {
    let table = TableIdent::from_strs(&["default"], "users");
    assert_eq!(table.to_string(), "default.users");
}
