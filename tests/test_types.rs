use icepick::spec::types::{NestedField, PrimitiveType, StructType, Type};

#[test]
fn test_primitive_type_boolean() {
    let t = Type::Primitive(PrimitiveType::Boolean);
    assert!(matches!(t, Type::Primitive(PrimitiveType::Boolean)));
}

#[test]
fn test_primitive_type_integer() {
    let t = Type::Primitive(PrimitiveType::Int);
    assert!(matches!(t, Type::Primitive(PrimitiveType::Int)));
}

#[test]
fn test_primitive_type_long() {
    let t = Type::Primitive(PrimitiveType::Long);
    assert!(matches!(t, Type::Primitive(PrimitiveType::Long)));
}

#[test]
fn test_primitive_type_string() {
    let t = Type::Primitive(PrimitiveType::String);
    assert!(matches!(t, Type::Primitive(PrimitiveType::String)));
}

#[test]
fn test_primitive_type_binary() {
    let t = Type::Primitive(PrimitiveType::Binary);
    assert!(matches!(t, Type::Primitive(PrimitiveType::Binary)));
}

#[test]
fn test_nested_field_required() {
    let field = NestedField::new(
        1,
        "id".to_string(),
        Type::Primitive(PrimitiveType::Long),
        true,
        None,
    );

    assert_eq!(field.id(), 1);
    assert_eq!(field.name(), "id");
    assert!(field.is_required());
    assert!(matches!(
        field.field_type(),
        Type::Primitive(PrimitiveType::Long)
    ));
}

#[test]
fn test_nested_field_optional_with_doc() {
    let field = NestedField::new(
        2,
        "email".to_string(),
        Type::Primitive(PrimitiveType::String),
        false,
        Some("User email address".to_string()),
    );

    assert_eq!(field.id(), 2);
    assert!(!field.is_required());
    assert_eq!(field.doc(), Some("User email address"));
}

#[test]
fn test_struct_type_simple() {
    let fields = vec![
        NestedField::required_field(1, "id".to_string(), Type::Primitive(PrimitiveType::Long)),
        NestedField::required_field(
            2,
            "name".to_string(),
            Type::Primitive(PrimitiveType::String),
        ),
    ];

    let struct_type = StructType::new(fields.clone());
    assert_eq!(struct_type.fields().len(), 2);
    assert_eq!(struct_type.fields()[0].name(), "id");
    assert_eq!(struct_type.fields()[1].name(), "name");
}

#[test]
fn test_struct_type_nested() {
    let address_fields = vec![
        NestedField::required_field(
            3,
            "street".to_string(),
            Type::Primitive(PrimitiveType::String),
        ),
        NestedField::optional_field(
            4,
            "city".to_string(),
            Type::Primitive(PrimitiveType::String),
        ),
    ];

    let fields = vec![
        NestedField::required_field(1, "id".to_string(), Type::Primitive(PrimitiveType::Long)),
        NestedField::optional_field(
            2,
            "address".to_string(),
            Type::Struct(StructType::new(address_fields)),
        ),
    ];

    let struct_type = StructType::new(fields);
    assert_eq!(struct_type.fields().len(), 2);

    // Check nested struct
    if let Type::Struct(nested) = struct_type.fields()[1].field_type() {
        assert_eq!(nested.fields().len(), 2);
    } else {
        panic!("Expected nested struct");
    }
}
