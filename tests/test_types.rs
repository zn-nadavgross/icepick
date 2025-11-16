use icepick::spec::types::{PrimitiveType, Type};

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
