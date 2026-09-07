import assert from 'node:assert'
import { RRuleTemporal } from 'rrule-temporal'

/**
 * The DTSTART a rule that carries none of its own recurs from.
 *
 * A recurrence rule is not a complete recurrence on its own: RFC 5545 takes the phase of every
 * INTERVAL, and the time of day of any component no BYxxx part pins, from DTSTART. Most callers
 * write `FREQ=DAILY;BYHOUR=9` and mean "every day at nine", so one has to be supplied, and it has
 * to be the same one in every process and every release: a schedule is evaluated by whichever
 * instance runs the pass, and an anchor of "now" would move the phase of `INTERVAL=2` on every
 * restart.
 *
 * Left floating (no TZID) so the schedule's own `tz` resolves it: the anchor is midnight in the
 * caller's zone rather than midnight in UTC.
 */
const EPOCH_DTSTART = 'DTSTART:19700101T000000'

/**
 * The properties an expression may contain. Anything else is rejected rather than ignored: a
 * mistyped `DTSRAT` would otherwise be dropped silently and leave the rule anchored on the epoch,
 * which is a schedule that runs at the wrong time rather than one that reports a problem.
 */
const PROPERTIES = new Set(['DTSTART', 'RRULE', 'RDATE', 'EXDATE'])

/**
 * The recur rule parts of RFC 5545 3.3.10, plus RSCALE and SKIP from RFC 7529. Unknown parts are
 * rejected for the same reason unknown properties are: `BYHOURS=9` is not an hour of the day to any
 * parser, and evaluating it as `FREQ=DAILY` alone would put the job at midnight without a word.
 */
const PARTS = new Set([
  'FREQ', 'UNTIL', 'COUNT', 'INTERVAL', 'BYSECOND', 'BYMINUTE', 'BYHOUR', 'BYDAY', 'BYMONTHDAY',
  'BYYEARDAY', 'BYWEEKNO', 'BYMONTH', 'BYSETPOS', 'WKST', 'RSCALE', 'SKIP'
])

/** The property a line names, or nothing at all for a bare recurrence rule. */
const PROPERTY = /^([a-z][a-z0-9-]*)[;:]/i

/**
 * What tells a recurrence rule from a cron expression: an iCalendar property at the head of the
 * expression, or the FREQ part every RRULE is required to carry. A cron expression has neither, and
 * cannot: no cron field contains `=`.
 */
const RRULE_SHAPE = /^\s*(?:DTSTART|RRULE|RDATE|EXDATE)[;:]|(?:^|[\s;])FREQ=/i

/** True if `expression` is a recurrence rule rather than a cron expression. */
export function isRrule (expression: string): boolean {
  return RRULE_SHAPE.test(expression)
}

/**
 * Rewrites an expression as the iCalendar text rrule-temporal parses, and rejects what it would
 * otherwise read past.
 *
 * Two shapes reach this: the RRULE property's value on its own (`FREQ=DAILY;BYHOUR=9`), which is
 * what a caller reaching for a cron replacement writes, and the iCalendar block a calendar exports
 * (`DTSTART;TZID=Europe/Berlin:20260901T090000` and an `RRULE:` line, optionally with RDATE and
 * EXDATE). Both end up as the latter.
 */
function toIcs (expression: string): string {
  const lines: string[] = []

  let dtstart = false
  let rule: string | null = null

  // A line break followed by a space or tab is a fold, not a new property (RFC 5545 3.1), and an
  // export with a long EXDATE list arrives folded. Unfolded here so a continuation is not read as
  // a property of its own and rejected as one.
  for (const line of expression.replace(/\r?\n[ \t]/g, '').split(/\r?\n/)) {
    const text = line.trim()

    if (text.length === 0) {
      continue
    }

    const property = PROPERTY.exec(text)?.[1].toUpperCase()

    // No property name means the line is the rule itself, which is exactly the value half of an
    // RRULE line, so naming it is all that is needed to make the expression parseable.
    const name = property || 'RRULE'
    const value = property ? text.slice(text.indexOf(':') + 1) : text

    assert(PROPERTIES.has(name),
      `Unsupported property "${name}" in rrule expression. Supported properties: ${[...PROPERTIES].join(', ')}`)

    if (name === 'DTSTART') {
      // A second one does not merge with the first, it replaces it, so an expression carrying two
      // has one of them evaluated and the other quietly discarded.
      assert(!dtstart, 'rrule expression has more than one DTSTART')
      dtstart = true
    }

    if (name === 'RRULE') {
      assert(rule === null, 'rrule expression has more than one RRULE')
      rule = value
      assertParts(value)
    }

    lines.push(property ? text : `RRULE:${text}`)
  }

  if (!dtstart) {
    // COUNT counts from DTSTART, so on the epoch anchor every count worth having is long spent: the
    // rule parses, has no occurrence left, and the schedule it lands on never sends anything.
    // Rejected here because a schedule that silently does nothing is the one failure a caller
    // cannot see.
    assert(!/(^|;)COUNT=/i.test(rule || ''),
      'rrule expression uses COUNT, which counts occurrences from DTSTART, so it needs a DTSTART of its own')

    lines.unshift(EPOCH_DTSTART)
  }

  return lines.join('\n')
}

/** Rejects a recur rule part no parser reads, which would otherwise change the schedule silently. */
function assertParts (rule: string): void {
  for (const part of rule.split(';')) {
    if (part.length === 0) {
      continue
    }

    const [name] = part.toUpperCase().split('=')

    // X- names are the extension mechanism RFC 5545 3.3.10 leaves open, so they are passed through
    // to the parser rather than judged here.
    assert(PARTS.has(name) || name.startsWith('X-'), `Unsupported part "${part}" in rrule expression`)
  }
}

function buildRule (expression: string, tz: string): RRuleTemporal {
  // strict enforces the combinations RFC 5545 3.3.10 forbids outright (BYMONTHDAY with a weekly
  // frequency, an ordinal BYDAY on anything but a monthly or yearly one). Without it a rule that no
  // two engines would agree on is evaluated anyway, and a schedule fires at a time its author has
  // no way to predict.
  return new RRuleTemporal({ rruleString: toIcs(expression), tzid: tz, strict: true })
}

/**
 * The first occurrence strictly after `after`, or null when the rule has none: an exhausted COUNT
 * or a passed UNTIL leaves a schedule with nothing left to send.
 *
 * The time zone comes from DTSTART when it names one, and from `tz` otherwise, which is both what
 * RFC 5545 says and what lets a rule written without a DTSTART behave like a cron expression on the
 * same schedule.
 */
export function nextOccurrence (expression: string, after: Date, tz: string): Date | null {
  const occurrence = buildRule(expression, tz).next(after)

  // The cron pass compares occurrences against a Date, so the nanoseconds a Temporal instant
  // carries have nowhere to go. Nothing is lost: an iCalendar DTSTART is second-precision, and
  // every occurrence is derived from it.
  return occurrence === null ? null : new Date(occurrence.epochMilliseconds)
}

/**
 * Rejects an expression or time zone `schedule()` should not store, so a schedule that cannot be
 * evaluated is reported to the caller rather than to a warning on every later pass.
 */
export function assertRrule (expression: string, tz: string): void {
  // The expression first, against a zone known to be usable, so a bad expression reports as one
  // rather than as a time zone problem.
  buildRule(expression, 'UTC')

  try {
    buildRule(expression, tz)
  } catch {
    // The expression has already parsed once, so the zone is the only thing left to blame. Reported
    // in the same words cron scheduling uses for the same mistake, rather than as the uppercased
    // "Invalid time zone specified" a Temporal implementation throws.
    throw new Error(`Unknown or unsupported time zone: "${tz}"`)
  }
}
