//! Recurring-order scheduling helpers (pure, no I/O).

use chrono::{DateTime, Duration, Months, Utc};

use crate::types::IntervalType;

/// Compute the next execution timestamp for a recurring order: `from` advanced
/// by `interval_value` units of `interval_type`. `interval_value` is clamped to
/// at least 1 (a zero/negative cadence would never advance).
///
/// Hourly/Daily/Weekly use fixed `Duration` arithmetic; Monthly uses calendar
/// months via `checked_add_months`, which clamps end-of-month overflow (e.g.
/// Jan 31 + 1 month → Feb 28/29). On the (impossible-in-practice) overflow of
/// the calendar arithmetic, falls back to a 30-day-per-month approximation so
/// the function is total.
#[must_use]
pub fn next_execution_at(
    from: DateTime<Utc>,
    interval_type: IntervalType,
    interval_value: i32,
) -> DateTime<Utc> {
    let n = interval_value.max(1);
    match interval_type {
        IntervalType::Hourly => from + Duration::hours(i64::from(n)),
        IntervalType::Daily => from + Duration::days(i64::from(n)),
        IntervalType::Weekly => from + Duration::weeks(i64::from(n)),
        IntervalType::Monthly => {
            let months = u32::try_from(n).unwrap_or(1);
            from.checked_add_months(Months::new(months))
                .unwrap_or_else(|| from + Duration::days(30 * i64::from(n)))
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn at(y: i32, m: u32, d: u32, h: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, h, 0, 0).unwrap()
    }

    #[test]
    fn hourly_advances_hours() {
        let base = at(2026, 6, 7, 10);
        assert_eq!(
            next_execution_at(base, IntervalType::Hourly, 1),
            at(2026, 6, 7, 11)
        );
        assert_eq!(
            next_execution_at(base, IntervalType::Hourly, 6),
            at(2026, 6, 7, 16)
        );
    }

    #[test]
    fn daily_advances_days() {
        let base = at(2026, 6, 7, 10);
        assert_eq!(
            next_execution_at(base, IntervalType::Daily, 1),
            at(2026, 6, 8, 10)
        );
        assert_eq!(
            next_execution_at(base, IntervalType::Daily, 3),
            at(2026, 6, 10, 10)
        );
    }

    #[test]
    fn weekly_advances_weeks() {
        let base = at(2026, 6, 7, 10);
        assert_eq!(
            next_execution_at(base, IntervalType::Weekly, 1),
            at(2026, 6, 14, 10)
        );
        assert_eq!(
            next_execution_at(base, IntervalType::Weekly, 2),
            at(2026, 6, 21, 10)
        );
    }

    #[test]
    fn monthly_advances_calendar_months() {
        let base = at(2026, 1, 15, 10);
        assert_eq!(
            next_execution_at(base, IntervalType::Monthly, 1),
            at(2026, 2, 15, 10)
        );
        assert_eq!(
            next_execution_at(base, IntervalType::Monthly, 3),
            at(2026, 4, 15, 10)
        );
    }

    #[test]
    fn monthly_clamps_end_of_month() {
        // Jan 31 + 1 month → Feb 28 (2026 not a leap year).
        let base = at(2026, 1, 31, 10);
        assert_eq!(
            next_execution_at(base, IntervalType::Monthly, 1),
            at(2026, 2, 28, 10)
        );
    }

    #[test]
    fn zero_or_negative_interval_clamps_to_one() {
        let base = at(2026, 6, 7, 10);
        assert_eq!(
            next_execution_at(base, IntervalType::Daily, 0),
            at(2026, 6, 8, 10)
        );
        assert_eq!(
            next_execution_at(base, IntervalType::Daily, -5),
            at(2026, 6, 8, 10)
        );
    }
}
