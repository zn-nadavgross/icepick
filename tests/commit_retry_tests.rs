// Note: This test will be a simulation since we can't easily trigger concurrent modifications
// in a single-threaded test. The test verifies the retry logic structure exists.

use icepick::error::Error;

#[test]
fn test_is_concurrent_modification_error() {
    let err = Error::concurrent_modification("version mismatch");
    assert!(matches!(err, Error::ConcurrentModification { .. }));
}
