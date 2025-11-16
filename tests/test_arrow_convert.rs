use icepick::arrow_convert::schema_to_arrow;
use icepick::spec::{NestedField, PrimitiveType, Schema, Type};

#[test]
fn test_schema_to_arrow_simple() {
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required_field(1, "id".to_string(), Type::Primitive(PrimitiveType::Long)),
            NestedField::optional_field(
                2,
                "name".to_string(),
                Type::Primitive(PrimitiveType::String),
            ),
        ])
        .build()
        .unwrap();

    let arrow_schema = schema_to_arrow(&schema).unwrap();

    assert_eq!(arrow_schema.fields().len(), 2);
    assert_eq!(arrow_schema.field(0).name(), "id");
    assert_eq!(arrow_schema.field(1).name(), "name");
    assert!(!arrow_schema.field(0).is_nullable());
    assert!(arrow_schema.field(1).is_nullable());
}
