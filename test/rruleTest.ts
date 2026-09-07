import { expect } from 'vitest'
import { delay } from '../src/tools.ts'
import * as helper from './testHelper.ts'
import { PgBoss } from '../src/index.ts'
import { rruleRecurrence } from '../src/rrule.ts'
import { ctx } from './hooks.ts'

// The parser under test is a pure function of (expression, after, tz), so most of these need no
// database at all: they ask it for an occurrence and check the instant it names.
const AFTER = new Date('2026-09-07T00:00:00Z')

function next (expression: string, tz = 'UTC', after: Date = AFTER): string | null {
  const occurrence = rruleRecurrence.next(expression, after, tz)

  return occurrence ? occurrence.toISOString() : null
}

function validate (expression: string, tz = 'UTC') {
  rruleRecurrence.validate!(expression, tz)
}

async function readSchedule () {
  const db = await helper.getDb()

  const { rows } = await db.executeSql(
    `SELECT kind, cron, next_run_at as "nextRunAt", last_run_at as "lastRunAt"
     FROM ${ctx.schema}.schedule WHERE name = $1`,
    [ctx.schema]
  )

  await db.close()

  return rows[0]
}

// Fetches until `expected` jobs have arrived or the deadline passes, so a test can assert on both
// too few and too many.
async function collectJobs (expected: number, timeoutMs = 8000) {
  const collected: unknown[] = []
  const deadline = Date.now() + timeoutMs

  while (Date.now() < deadline && collected.length < expected) {
    const jobs = await ctx.boss!.fetch(ctx.schema, { batchSize: 10 })

    collected.push(...jobs)

    if (collected.length < expected) {
      await delay(250)
    }
  }

  return collected
}

describe('rrule', function () {
  it('reads a bare recurrence rule in the schedule time zone', function () {
    // The RRULE value on its own is what a caller reaching for a cron replacement writes, and the
    // zone it recurs in is the schedule's.
    expect(next('FREQ=DAILY;BYHOUR=9')).toBe('2026-09-07T09:00:00.000Z')
    expect(next('FREQ=DAILY;BYHOUR=9', 'America/Chicago')).toBe('2026-09-07T14:00:00.000Z')
    expect(next('RRULE:FREQ=DAILY;BYHOUR=9')).toBe('2026-09-07T09:00:00.000Z')
  })

  it('anchors a rule with no DTSTART on the epoch, so every instance agrees on the phase', function () {
    // Midnight on 1970-01-01 in the schedule's own zone, which is what makes an INTERVAL land on
    // the same instants whichever instance runs the pass and whenever the schedule was created.
    expect(next('FREQ=HOURLY;INTERVAL=6')).toBe('2026-09-07T06:00:00.000Z')
    expect(next('FREQ=MINUTELY;INTERVAL=30', 'UTC', new Date('2026-09-07T00:07:00Z')))
      .toBe('2026-09-07T00:30:00.000Z')
    expect(next('FREQ=DAILY')).toBe('2026-09-08T00:00:00.000Z')
  })

  it('takes the zone from DTSTART when it names one', function () {
    // 09:00 in Berlin, whatever the schedule was given, because the expression is explicit about it
    expect(next('DTSTART;TZID=Europe/Berlin:20260901T090000\nRRULE:FREQ=DAILY', 'America/Chicago'))
      .toBe('2026-09-07T07:00:00.000Z')

    // and 09:00 in the schedule's zone when the expression leaves it floating
    expect(next('DTSTART:20260901T090000\nRRULE:FREQ=DAILY', 'America/Chicago'))
      .toBe('2026-09-07T14:00:00.000Z')
  })

  it('accepts the iCalendar shape a calendar exports', function () {
    expect(next('DTSTART:20260907T090000Z\nRRULE:FREQ=DAILY\nEXDATE:20260907T090000Z'))
      .toBe('2026-09-08T09:00:00.000Z')

    expect(next('DTSTART:20260101T090000Z\nRRULE:FREQ=YEARLY\nRDATE:20260908T050000Z'))
      .toBe('2026-09-08T05:00:00.000Z')

    // property names are case insensitive, and an export arrives with CRLF line endings
    expect(next('dtstart:20260901T090000Z\r\nrrule:freq=daily')).toBe('2026-09-07T09:00:00.000Z')

    // a long line arrives folded, which is a line break and a space rather than a new property
    expect(next('DTSTART:20260901T090000Z\r\nRRULE:FREQ=DAILY\r\nEXDATE:20260907T090000Z,\r\n 20260908T090000Z'))
      .toBe('2026-09-09T09:00:00.000Z')
  })

  it('resolves occurrences finer than a minute', function () {
    expect(next('FREQ=SECONDLY;BYSECOND=15,45')).toBe('2026-09-07T00:00:15.000Z')
    expect(next('FREQ=SECONDLY;INTERVAL=15')).toBe('2026-09-07T00:00:15.000Z')
  })

  it('answers with an occurrence strictly after the one it is given', function () {
    // What the cron pass walks a catch-up run with: an answer at or before `after` would spin it
    const first = rruleRecurrence.next('FREQ=DAILY;BYHOUR=9', AFTER, 'UTC') as Date
    const second = rruleRecurrence.next('FREQ=DAILY;BYHOUR=9', first, 'UTC') as Date

    expect(first.toISOString()).toBe('2026-09-07T09:00:00.000Z')
    expect(second.toISOString()).toBe('2026-09-08T09:00:00.000Z')
  })

  it('reports a finite rule with nothing left as having no further occurrence', function () {
    expect(next('DTSTART:20200101T090000Z\nRRULE:FREQ=DAILY;UNTIL=20200201T090000Z')).toBeNull()
    expect(next('DTSTART:20200101T090000Z\nRRULE:FREQ=DAILY;COUNT=3')).toBeNull()
    expect(next('DTSTART:20261001T090000Z\nRRULE:FREQ=DAILY;COUNT=3')).toBe('2026-10-01T09:00:00.000Z')
  })

  it('keeps the wall clock time across a daylight saving transition', function () {
    // 09:00 in Berlin on either side of the March transition, an hour apart in UTC
    expect(next('DTSTART:20260301T090000\nRRULE:FREQ=DAILY', 'Europe/Berlin', new Date('2026-03-27T12:00:00Z')))
      .toBe('2026-03-28T08:00:00.000Z')
    expect(next('DTSTART:20260301T090000\nRRULE:FREQ=DAILY', 'Europe/Berlin', new Date('2026-03-28T12:00:00Z')))
      .toBe('2026-03-29T07:00:00.000Z')
  })

  it('rejects COUNT without a DTSTART to count from', function () {
    // On the epoch anchor every count worth having is long spent, so the schedule would parse and
    // then never fire.
    expect(() => validate('FREQ=DAILY;COUNT=3')).toThrow(/COUNT/)
    expect(() => validate('DTSTART:20261001T090000Z\nRRULE:FREQ=DAILY;COUNT=3')).not.toThrow()
  })

  it('rejects a part no parser reads rather than evaluating the rest', function () {
    expect(() => validate('FREQ=DAILY;BYHOURS=9')).toThrow('Unsupported part "BYHOURS=9" in rrule expression')
    expect(() => validate('every day at nine')).toThrow(/Unsupported part/)
    // X- names are the extension mechanism RFC 5545 leaves open
    expect(() => validate('FREQ=DAILY;X-FOO=1;BYHOUR=9')).not.toThrow()
  })

  it('rejects a property no parser reads, which would otherwise change the anchor', function () {
    // A mistyped DTSTART would be dropped and leave the rule anchored on the epoch
    expect(() => validate('DTSRAT:20260901T090000Z\nRRULE:FREQ=DAILY'))
      .toThrow('Unsupported property "DTSRAT" in rrule expression. Supported properties: DTSTART, RRULE, RDATE, EXDATE')

    expect(() => validate('SUMMARY:standup\nRRULE:FREQ=DAILY')).toThrow(/Unsupported property/)
  })

  it('rejects a second DTSTART or RRULE instead of quietly dropping one', function () {
    expect(() => validate('RRULE:FREQ=DAILY\nRRULE:FREQ=WEEKLY')).toThrow('rrule expression has more than one RRULE')
    expect(() => validate('DTSTART:20260101T090000Z\nDTSTART:20260102T090000Z\nRRULE:FREQ=DAILY'))
      .toThrow('rrule expression has more than one DTSTART')
  })

  it('rejects a rule RFC 5545 forbids, whose reading no two engines agree on', function () {
    expect(() => validate('FREQ=WEEKLY;BYMONTHDAY=1')).toThrow(/BYMONTHDAY MUST NOT be used when FREQ is WEEKLY/)
    expect(() => validate('FREQ=DAILY;BYDAY=1MO')).toThrow(/MUST NOT/)
    expect(() => validate('FREQ=NOPE')).toThrow(/Invalid FREQ value/)
    expect(() => validate('FREQ=DAILY;INTERVAL=0')).toThrow(/interval must be greater than 0/)
  })

  it('rejects an unusable time zone in the same words the cron kind uses', function () {
    expect(() => validate('FREQ=DAILY;BYHOUR=9', 'America/New_Yrok'))
      .toThrow('Unknown or unsupported time zone: "America/New_Yrok"')

    // the expression is judged first, so a caller with two mistakes hears about the rule
    expect(() => validate('FREQ=NOPE', 'America/New_Yrok')).toThrow(/Invalid FREQ value/)
  })

  it('is built in, so a schedule needs no parser registered for it', async function () {
    const config = {
      ...ctx.bossConfig,
      cronMonitorIntervalSeconds: 1,
      cronWorkerIntervalSeconds: 1,
      schedule: true
    }

    ctx.boss = await helper.start(config)

    await ctx.boss.schedule(ctx.schema, { kind: 'rrule', expression: 'FREQ=MINUTELY' })

    const jobs = await collectJobs(1)

    expect(jobs.length).toBe(1)

    const schedule = await readSchedule()

    expect(schedule.kind).toBe('rrule')
    // the expression lives in the cron column whatever the kind
    expect(schedule.cron).toBe('FREQ=MINUTELY')
    expect(schedule.lastRunAt).toBeTruthy()
  })

  it('sends every occurrence of a rule that recurs faster than a minute', async function () {
    const config = {
      ...ctx.bossConfig,
      cronMonitorIntervalSeconds: 1,
      cronWorkerIntervalSeconds: 1,
      schedule: true
    }

    ctx.boss = await helper.start(config)

    await ctx.boss.schedule(ctx.schema, { kind: 'rrule', expression: 'FREQ=SECONDLY;INTERVAL=2' })

    const jobs = await collectJobs(2)

    expect(jobs.length).toBeGreaterThanOrEqual(2)
  })

  it('reports the kind and the expression through getSchedules', async function () {
    ctx.boss = await helper.start(ctx.bossConfig)

    const expression = 'DTSTART;TZID=Europe/Berlin:20260901T090000\nRRULE:FREQ=WEEKLY;BYDAY=MO,WE,FR'

    await ctx.boss.schedule(ctx.schema, { kind: 'rrule', expression }, null, { tz: 'Europe/Berlin' })

    const [schedule] = await ctx.boss.getSchedules()

    expect(schedule.kind).toBe('rrule')
    expect(schedule.expression).toBe(expression)
    expect(schedule.timezone).toBe('Europe/Berlin')
    expect(schedule.nextRunAt).toBeTruthy()
  })

  it('refuses an unusable expression at schedule() time rather than storing it', async function () {
    ctx.boss = await helper.start(ctx.bossConfig)

    await expect(ctx.boss.schedule(ctx.schema, { kind: 'rrule', expression: 'FREQ=DAILY;BYHOURS=9' }))
      .rejects.toThrow(/Unsupported part/)

    await expect(ctx.boss.schedule(ctx.schema, { kind: 'rrule', expression: 'FREQ=DAILY' }, null, { tz: 'Nowhere/Special' }))
      .rejects.toThrow(/Unknown or unsupported time zone/)

    expect(await ctx.boss.getSchedules()).toHaveLength(0)
  })

  it('refuses to let a registered parser replace the built-in rrule kind', function () {
    expect(() => new PgBoss({
      ...ctx.bossConfig,
      recurrences: { rrule: { next: () => new Date() } }
    })).toThrow(/"rrule" is built in/)
  })
})
