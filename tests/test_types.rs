use icepick::spec::types::{ListType, MapType, NestedField, PrimitiveType, StructType, Type};
use serde_json::{json, Value};

fn get_type_field(value: &Value) -> Option<&str> {
    value.get("type")?.as_str()
}

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

#[test]
fn test_struct_type_serializes_with_type_field() {
    let struct_type = StructType::new(vec![NestedField::required_field(
        1,
        "field".to_string(),
        Type::Primitive(PrimitiveType::Int),
    )]);
    let json = serde_json::to_value(&struct_type).unwrap();
    assert_eq!(get_type_field(&json), Some("struct"));
}

#[test]
fn test_list_type_serializes_with_type_field() {
    let list_type = ListType::new(2, true, Type::Primitive(PrimitiveType::String));
    let json = serde_json::to_value(&list_type).unwrap();
    assert_eq!(get_type_field(&json), Some("list"));
    assert!(json.get("element-id").is_some());
    assert!(json.get("element-required").is_some());
    assert!(json.get("element").is_some());
}

#[test]
fn test_map_type_serializes_with_type_field() {
    let map_type = MapType::new(
        3,
        Type::Primitive(PrimitiveType::String),
        4,
        false,
        Type::Primitive(PrimitiveType::Long),
    );
    let json = serde_json::to_value(&map_type).unwrap();
    assert_eq!(get_type_field(&json), Some("map"));
    assert!(json.get("key-id").is_some());
    assert!(json.get("key").is_some());
    assert!(json.get("value-id").is_some());
    assert!(json.get("value-required").is_some());
    assert!(json.get("value").is_some());
}

#[test]
fn test_primitive_string_serializes_as_str() {
    let json = serde_json::to_value(Type::Primitive(PrimitiveType::String)).unwrap();
    assert_eq!(json, Value::String("string".to_string()));
}

#[test]
fn test_primitive_fixed_serializes_to_string() {
    let json = serde_json::to_value(Type::Primitive(PrimitiveType::Fixed(16))).unwrap();
    assert_eq!(json, Value::String("fixed[16]".to_string()));
}

#[test]
fn test_primitive_decimal_serializes_to_string() {
    let json = serde_json::to_value(Type::Primitive(PrimitiveType::Decimal {
        precision: 10,
        scale: 2,
    }))
    .unwrap();
    assert_eq!(json, Value::String("decimal(10,2)".to_string()));
}

#[test]
fn test_primitive_fixed_deserializes_from_spec_json() {
    let value = json!({"type": "fixed", "length": 4});
    let ty: Type = serde_json::from_value(value).unwrap();
    assert!(matches!(ty, Type::Primitive(PrimitiveType::Fixed(4))));
}

#[test]
fn test_primitive_decimal_deserializes_from_spec_json() {
    let value = json!({"type": "decimal", "precision": 5, "scale": 3});
    let ty: Type = serde_json::from_value(value).unwrap();
    assert!(matches!(
        ty,
        Type::Primitive(PrimitiveType::Decimal {
            precision: 5,
            scale: 3
        })
    ));
}
