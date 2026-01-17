//! Date arithmetic utilities for Iceberg date types
//!
//! This module provides functions for converting between dates and
//! days since Unix epoch, which is the standard Iceberg date representation.

/// Convert a year to days since Unix epoch (1970-01-01)
///
/// Returns the number of days from 1970-01-01 to January 1st of the given year.
pub fn year_to_days(year: i32) -> i32 {
    let y = year - 1970;
    if y >= 0 {
        y * 365 + (y + 1) / 4 - (y + 69) / 100 + (y + 369) / 400
    } else {
        y * 365 + y / 4 - (y - 31) / 100 + (y - 31) / 400
    }
}

/// Convert days since Unix epoch to year
pub fn days_to_year(days: i32) -> i32 {
    // Approximate year, then adjust
    let mut year = 1970 + days / 365;

    loop {
        let year_start = year_to_days(year);
        if year_start > days {
            year -= 1;
        } else {
            let next_year_start = year_to_days(year + 1);
            if next_year_start <= days {
                year += 1;
            } else {
                break;
            }
        }
    }

    year
}

/// Convert days since Unix epoch to (year, month) where month is 1-12
pub fn days_to_year_month(days: i32) -> (i32, i32) {
    let year = days_to_year(days);
    let year_start = year_to_days(year);
    let day_of_year = days - year_start;

    let is_leap = is_leap_year(year);
    let days_in_months: [i32; 12] = if is_leap {
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };

    let mut remaining = day_of_year;
    for (i, &days_in_month) in days_in_months.iter().enumerate() {
        if remaining < days_in_month {
            return (year, i as i32 + 1);
        }
        remaining -= days_in_month;
    }

    (year, 12)
}

/// Check if a year is a leap year
pub fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

/// Parse a date string like "2024-01-15" to days since Unix epoch
pub fn parse_date_to_days(s: &str) -> Option<i32> {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 3 {
        return None;
    }

    let year: i32 = parts[0].parse().ok()?;
    let month: i32 = parts[1].parse().ok()?;
    let day: i32 = parts[2].parse().ok()?;

    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }

    let year_days = year_to_days(year);
    let is_leap = is_leap_year(year);
    let days_before_month: [i32; 12] = if is_leap {
        [0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335]
    } else {
        [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334]
    };

    Some(year_days + days_before_month[(month - 1) as usize] + day - 1)
}

/// Parse a date string like "2024-01-15" to year
pub fn parse_date_year(s: &str) -> Option<i32> {
    let parts: Vec<&str> = s.split('-').collect();
    if !parts.is_empty() {
        parts[0].parse().ok()
    } else {
        None
    }
}

/// Parse a date string like "2024-01-15" to (year, month)
pub fn parse_date_year_month(s: &str) -> Option<(i32, i32)> {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() >= 2 {
        let year: i32 = parts[0].parse().ok()?;
        let month: i32 = parts[1].parse().ok()?;
        Some((year, month))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_year_to_days() {
        // 1970-01-01 is day 0
        assert_eq!(year_to_days(1970), 0);
        // 1971-01-01 is day 365
        assert_eq!(year_to_days(1971), 365);
        // 2000-01-01 (30 years, 7 leap years)
        assert_eq!(year_to_days(2000), 10957);
    }

    #[test]
    fn test_days_to_year() {
        assert_eq!(days_to_year(0), 1970);
        assert_eq!(days_to_year(365), 1971);
        assert_eq!(days_to_year(10957), 2000);
    }

    #[test]
    fn test_is_leap_year() {
        assert!(!is_leap_year(1970));
        assert!(is_leap_year(2000));
        assert!(!is_leap_year(1900));
        assert!(is_leap_year(2024));
    }

    #[test]
    fn test_parse_date_to_days() {
        // 2024-01-01 should be consistent
        let days = parse_date_to_days("2024-01-01").unwrap();
        assert_eq!(days_to_year(days), 2024);

        // Invalid dates
        assert!(parse_date_to_days("invalid").is_none());
        assert!(parse_date_to_days("2024-13-01").is_none());
    }

    #[test]
    fn test_days_to_year_month() {
        // 2024-01-15
        let days = parse_date_to_days("2024-01-15").unwrap();
        assert_eq!(days_to_year_month(days), (2024, 1));

        // 2024-06-15
        let days = parse_date_to_days("2024-06-15").unwrap();
        assert_eq!(days_to_year_month(days), (2024, 6));
    }
}
