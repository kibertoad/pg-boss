import { CronExpressionParser } from 'cron-parser'

/**
 * Asserts that `tz` is a time zone scheduling can actually use.
 *
 * cron-parser validates `tz` lazily: parsing without a reference date never constructs a CronDate,
 * so every string is accepted and a bad zone only surfaces later, when a date is computed, as an
 * opaque "CronDate: unhandled timestamp". Passing a reference date here forces that construction so
 * a typo like 'America/New_Yrok' is rejected by schedule() rather than persisted to the schedule
 * table. Deliberately reuses cron-parser rather than an independent Intl check, so what schedule()
 * accepts is exactly what the cron pass can evaluate.
 *
 * A recurrence rule is evaluated by rrule-temporal rather than cron-parser and is judged here all
 * the same: both resolve IANA zone names, and rrule-temporal ignores the zone it is handed whenever
 * DTSTART names one of its own, so nothing else judges the value the schedule stores.
 *
 * Callers validate the expression first, so a failure here is attributable to the zone.
 */
export function assertTimezone (tz: string): void {
  try {
    CronExpressionParser.parse('* * * * *', { tz, strict: false, currentDate: new Date() })
  } catch {
    // Quoted so an empty string renders as `""` rather than a dangling colon
    throw new Error(`Unknown or unsupported time zone: "${tz}"`)
  }
}
