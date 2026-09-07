import { expect } from 'vitest'
import { delay } from '../src/tools.ts'
import * as helper from './testHelper.ts'
import Timekeeper from '../src/timekeeper.ts'
import { isRrule, nextOccurrence, assertRrule } from '../src/rrule.ts'
import { ctx } from './hooks.ts'

// Most of this needs no database: an expression is read by a pure function of (expression, after,
// tz), and the cron pass decides due-ness from the clock, both of which a bare Timekeeper answers.
const AFTER = new Date('2026-09-07T00:00:00Z')

function next (expression: string, tz = 'UTC', after: Date = AFTER): string | null {
  const occurrence = nextOccurrence(expression, after, tz)

  return occurrence ? occurrence.toISOString() : null
}

// A Timekeeper with a database that only answers the clock query, which is all the pass needs to
// judge whether a schedule has come due.
function makeTk () {
  const db = {
    executeSql: async () => ({ rows: [{ time: String(Date.now()) }] })
  }

  return new Timekeeper(db as any, {} as any, { schema: 'test' } as any)
}

/** The 60-second throttle slot a forwarded job lands in, which is what collapses a repeat send. */
function slotOf (epochMs: number) {
  return Math.floor(epochMs / 60_000)
}

describe('rrule', function () {
  it('tells a recurrence rule from a cron expression', function () {
    expect(isRrule('FREQ=DAILY;BYHOUR=9')).toBe(true)
    expect(isRrule('RRULE:FREQ=DAILY')).toBe(true)
    expect(isRrule('DTSTART:20260901T090000Z\nRRULE:FREQ=DAILY')).toBe(true)
    expect(isRrule('WKST=SU;FREQ=WEEKLY;BYDAY=TU')).toBe(true)

    // no cron field can contain an `=`, so nothing that already worked is read as a rule
    expect(isRrule('* * * * *')).toBe(false)
    expect(isRrule('0 3 * * *')).toBe(false)
    expect(isRrule('30 30 3 * * *')).toBe(false)
    expect(isRrule('0 0 1 1 *')).toBe(false)
    expect(isRrule('*/2 * * * MON-FRI')).toBe(false)
  })

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

  it('answers with an occurrence strictly after the one it is given', function () {
    const first = nextOccurrence('FREQ=DAILY;BYHOUR=9', AFTER, 'UTC') as Date
    const second = nextOccurrence('FREQ=DAILY;BYHOUR=9', first, 'UTC') as Date

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
    // then never send anything.
    expect(() => assertRrule('FREQ=DAILY;COUNT=3', 'UTC')).toThrow(/COUNT/)
    expect(() => assertRrule('DTSTART:20261001T090000Z\nRRULE:FREQ=DAILY;COUNT=3', 'UTC')).not.toThrow()
  })

  it('rejects a part no parser reads rather than evaluating the rest', function () {
    expect(() => assertRrule('FREQ=DAILY;BYHOURS=9', 'UTC'))
      .toThrow('Unsupported part "BYHOURS=9" in rrule expression')

    // X- names are the extension mechanism RFC 5545 leaves open
    expect(() => assertRrule('FREQ=DAILY;X-FOO=1;BYHOUR=9', 'UTC')).not.toThrow()
  })

  it('rejects a property no parser reads, which would otherwise change the anchor', function () {
    // A mistyped DTSTART would be dropped and leave the rule anchored on the epoch
    expect(() => assertRrule('DTSRAT:20260901T090000Z\nRRULE:FREQ=DAILY', 'UTC'))
      .toThrow('Unsupported property "DTSRAT" in rrule expression. Supported properties: DTSTART, RRULE, RDATE, EXDATE')

    expect(() => assertRrule('SUMMARY:standup\nRRULE:FREQ=DAILY', 'UTC')).toThrow(/Unsupported property/)
  })

  it('rejects a second DTSTART or RRULE instead of quietly dropping one', function () {
    expect(() => assertRrule('RRULE:FREQ=DAILY\nRRULE:FREQ=WEEKLY', 'UTC'))
      .toThrow('rrule expression has more than one RRULE')

    expect(() => assertRrule('DTSTART:20260101T090000Z\nDTSTART:20260102T090000Z\nRRULE:FREQ=DAILY', 'UTC'))
      .toThrow('rrule expression has more than one DTSTART')
  })

  it('rejects a rule RFC 5545 forbids, whose reading no two engines agree on', function () {
    expect(() => assertRrule('FREQ=WEEKLY;BYMONTHDAY=1', 'UTC'))
      .toThrow(/BYMONTHDAY MUST NOT be used when FREQ is WEEKLY/)
    expect(() => assertRrule('FREQ=DAILY;BYDAY=1MO', 'UTC')).toThrow(/MUST NOT/)
    expect(() => assertRrule('FREQ=NOPE', 'UTC')).toThrow(/Invalid FREQ value/)
    expect(() => assertRrule('FREQ=DAILY;INTERVAL=0', 'UTC')).toThrow(/interval must be greater than 0/)
  })

  it('rejects an unusable time zone in the same words a cron schedule does', function () {
    expect(() => assertRrule('FREQ=DAILY;BYHOUR=9', 'America/New_Yrok'))
      .toThrow('Unknown or unsupported time zone: "America/New_Yrok"')

    // the expression is judged first, so a caller with two mistakes hears about the rule
    expect(() => assertRrule('FREQ=NOPE', 'America/New_Yrok')).toThrow(/Invalid FREQ value/)
  })

  it('fires a rule whose occurrence falls inside the window, measured on the database clock', function () {
    const tk = makeTk()
    tk.clockSkew = 120_000 // db 2 minutes ahead of local

    // a minutely rule: the previous boundary is always less than 60s before database time, whatever
    // the skew, exactly as the equivalent cron expression behaves
    expect(tk.shouldSendIt('FREQ=MINUTELY', 'UTC')).toBe(true)
    expect(tk.shouldSendIt('FREQ=SECONDLY;INTERVAL=15', 'UTC')).toBe(true)
  })

  it('does not fire a rule with no occurrence in the window', function () {
    const tk = makeTk()

    // finished: the last occurrence is years back
    expect(tk.shouldSendIt('DTSTART:20200101T000000Z\nRRULE:FREQ=DAILY;UNTIL=20200201T000000Z', 'UTC')).toBe(false)

    // not started: the first occurrence is years out
    expect(tk.shouldSendIt('DTSTART:20991231T000000Z\nRRULE:FREQ=DAILY', 'UTC')).toBe(false)
  })

  it('files a rule occurrence in the throttle slot the occurrence falls in, not the one insert time does', async function () {
    const tk = makeTk()
    ;(tk as any).stopped = false

    const inserted: any[] = []
    ;(tk as any).manager = { insert: async (_q: string, jobs: any[]) => { inserted.push(...jobs) } }

    // Occurrences on the half minute, so two passes can find the same one inside the window from
    // opposite sides of a slot boundary.
    ;(tk as any).getSchedules = async () => ([
      { name: 'rule', key: '', data: null, options: {}, cron: 'FREQ=MINUTELY;BYSECOND=30', timezone: 'UTC' },
      { name: 'cron', key: '', data: null, options: {}, cron: '* * * * *', timezone: 'UTC' }
    ])

    // The most recent occurrence, which the skew below places the database clock relative to. Both
    // passes see this one: the window is a minute wide and the occurrences are a minute apart.
    const now = Date.now()
    const occurrence = Math.floor((now - 30_000) / 60_000) * 60_000 + 30_000

    // five seconds after the occurrence, in its own slot
    tk.clockSkew = occurrence + 5_000 - now
    await tk.cron()

    // and forty-five seconds after it, by which point insert time has moved into the next slot
    tk.clockSkew = occurrence + 45_000 - now
    await tk.cron()

    const [first, second] = inserted.filter(job => job.singletonKey === 'rule__')

    expect(first.singletonSeconds).toBe(60)
    expect(second.singletonSeconds).toBe(60)

    // Both passes file the occurrence in the slot it falls in, so the second job collapses into the
    // first instead of being sent as a job of its own.
    expect(slotOf(occurrence + 5_000 + first.singletonOffset * 1000)).toBe(slotOf(occurrence))
    expect(slotOf(occurrence + 45_000 + second.singletonOffset * 1000)).toBe(slotOf(occurrence))

    // A cron occurrence keeps the slot every release has always filed it in: during a rolling
    // upgrade an instance on an older release computes that slot and no other.
    for (const job of inserted.filter(job => job.singletonKey === 'cron__')) {
      expect(job.singletonSeconds).toBe(60)
      expect(job.singletonOffset).toBeUndefined()
    }
  })

  it('sends a job for a schedule created from a recurrence rule', async function () {
    const config = {
      ...ctx.bossConfig,
      cronMonitorIntervalSeconds: 1,
      cronWorkerIntervalSeconds: 1,
      schedule: true
    }

    ctx.boss = await helper.start(config)

    // Minutely, so an occurrence is always inside the window and the first pass sends a job, the
    // same way `* * * * *` does.
    await ctx.boss.schedule(ctx.schema, 'FREQ=MINUTELY')

    await delay(4000)

    const [job] = await ctx.boss.fetch(ctx.schema)

    expect(job).toBeTruthy()

    const [schedule] = await ctx.boss.getSchedules()

    expect(schedule.cron).toBe('FREQ=MINUTELY')
    expect(schedule.timezone).toBe('UTC')
  })

  it('refuses an unusable rule at schedule() time rather than storing it', async function () {
    ctx.boss = await helper.start(ctx.bossConfig)

    await expect(ctx.boss.schedule(ctx.schema, 'FREQ=DAILY;BYHOURS=9')).rejects.toThrow(/Unsupported part/)

    await expect(ctx.boss.schedule(ctx.schema, 'FREQ=DAILY', null, { tz: 'Nowhere/Special' }))
      .rejects.toThrow(/Unknown or unsupported time zone/)

    expect(await ctx.boss.getSchedules()).toHaveLength(0)
  })
})
