import { CronExpressionParser } from 'cron-parser'
import EventEmitter from 'node:events'

import * as Attorney from './attorney.ts'
import type Manager from './manager.ts'
import * as plans from './plans.ts'
import { isRrule, occurrencesInWindow, assertRrule } from './rrule.ts'
import { assertTimezone } from './timezone.ts'
import { delay } from './tools.ts'
import * as types from './types.ts'
import { emitAndPersistWarning, type WarningContext } from './warning.ts'

export const QUEUES = {
  SEND_IT: '__pgboss__send-it'
}

const EVENTS = {
  error: 'error',
  schedule: 'schedule',
  warning: 'warning'
}

const WARNINGS = {
  CLOCK_SKEW: {
    message: 'Warning: Clock skew between this instance and the database server. This will not break scheduling, but is emitted any time the skew exceeds 60 seconds.'
  }
}

const WARNING_TYPES = {
  CLOCK_SKEW: 'clock_skew',
  INVALID_SCHEDULE: 'invalid_schedule'
} as const

// How long an occurrence stays due, and the width of the throttle slot a forwarded job is filed in.
// One value because the two have to agree: a window wider than the slot lets two slots claim the
// same occurrence and send it twice, and a slot wider than the window collapses two occurrences a
// window apart into one job.
const OCCURRENCE_WINDOW_SECONDS = 60

// __singletonSlot is an internal field of the insert path rather than a documented send option, so
// the forwarded job widens JobInsert here rather than the type widening for everyone. The prefix is
// what keeps it internal: insert() stringifies caller objects straight into the recordset, so an
// unprefixed name would be a live, undeclared option on the public path.
type ForwardedJob = types.JobInsert & { __singletonSlot?: string }

/** What a schedule has come due for, and the format its expression was read in to find out. */
type DueOccurrences = { kind: types.ScheduleKind, occurrences: Date[] }

/**
 * The throttle slot an instant falls in, as the timestamp the insert files a job under.
 *
 * `singleton_on` is a timestamp without a zone holding UTC wall time, which is what the insert's own
 * slot expression computes from `now()` for a cron occurrence, so a slot measured here is rendered
 * in the same terms.
 */
function throttleSlot (instant: Date): string {
  const width = OCCURRENCE_WINDOW_SECONDS * 1000

  return new Date(Math.floor(instant.getTime() / width) * width).toISOString().replace('T', ' ').slice(0, 19)
}

class Timekeeper extends EventEmitter implements types.EventsMixin {
  db: types.IDatabase
  config: types.ResolvedConstructorOptions
  manager: Manager

  private stopped = true
  private cronMonitorInterval: NodeJS.Timeout | null | undefined
  private skewMonitorInterval: NodeJS.Timeout | null | undefined
  private timekeeping: boolean | undefined
  private _checkingSkew = false

  // Rows already warned about, keyed on (name, key, cron, timezone). Unlike every other warning
  // type, an unusable schedule never heals on its own: clock skew converges, a backlog drains, a
  // slow query is a one-off, but a bad row sits there until a human edits it. Warning every pass
  // would persist a row every cronMonitorIntervalSeconds forever, and warningRetentionDays has no
  // default, so a single typo could grow the warning table without bound. Rebuilt each pass from
  // the rows still broken, so a fixed or deleted schedule drops out and would warn again if it
  // came back.
  private warnedSchedules = new Set<string>()

  clockSkew = 0
  events = EVENTS

  constructor (db: types.IDatabase, manager: Manager, config: types.ResolvedConstructorOptions) {
    super()

    this.db = db
    this.config = config
    this.manager = manager
  }

  get checkingSkew (): boolean {
    return this._checkingSkew
  }

  private get warningContext (): WarningContext {
    return {
      emitter: this,
      db: this.db,
      schema: this.config.schema,
      persistWarnings: this.config.persistWarnings,
      warningEvent: this.events.warning,
      errorEvent: this.events.error
    }
  }

  async start () {
    this.stopped = false
    // A restart should re-surface a row nobody has fixed yet
    this.warnedSchedules.clear()

    await this.cacheClockSkew()
    await this.manager.createQueue(QUEUES.SEND_IT)

    const options = {
      pollingIntervalSeconds: this.config.cronWorkerIntervalSeconds,
      batchSize: 50
    }

    await this.manager.work<types.Request>(QUEUES.SEND_IT, options, (jobs) => this.onSendIt(jobs))

    setImmediate(() => this.onCron())

    this.cronMonitorInterval = setInterval(async () => await this.onCron(), this.config.cronMonitorIntervalSeconds! * 1000)
    this.skewMonitorInterval = setInterval(async () => await this.cacheClockSkew(), this.config.clockMonitorIntervalSeconds! * 1000)
  }

  async stop () {
    if (this.stopped) {
      return
    }

    this.stopped = true

    await this.manager.offWork(QUEUES.SEND_IT, { wait: true })

    if (this.skewMonitorInterval) {
      clearInterval(this.skewMonitorInterval)
      this.skewMonitorInterval = null
    }

    if (this.cronMonitorInterval) {
      clearInterval(this.cronMonitorInterval)
      this.cronMonitorInterval = null
    }

    while (this.timekeeping || this._checkingSkew) {
      await delay(10)
    }
  }

  async cacheClockSkew () {
    let skew = 0

    this._checkingSkew = true

    try {
      if (this.config.__test__force_clock_monitoring_error) {
        throw new Error(this.config.__test__force_clock_monitoring_error)
      }

      if (this.config.__test__delay_clock_skew_ms) {
        await delay(this.config.__test__delay_clock_skew_ms)
      }

      const { rows } = await this.db.executeSql(plans.getTime())

      const local = Date.now()

      const dbTime = parseFloat(rows[0].time)

      skew = dbTime - local

      const skewSeconds = Math.abs(skew) / 1000

      if (skewSeconds >= 60 || this.config.__test__force_clock_skew_warning) {
        await emitAndPersistWarning(
          this.warningContext,
          WARNING_TYPES.CLOCK_SKEW,
          WARNINGS.CLOCK_SKEW.message,
          { seconds: skewSeconds, direction: skew > 0 ? 'slower' : 'faster' }
        )
      }

      this.clockSkew = skew
    } catch (err) {
      this.emit(this.events.error, err)
    } finally {
      this._checkingSkew = false
    }
  }

  async onCron () {
    try {
      if (this.stopped || this.timekeeping) return

      if (this.config.__test__force_cron_monitoring_error) {
        throw new Error(this.config.__test__force_cron_monitoring_error)
      }

      this.timekeeping = true

      const sql = plans.trySetCronTime(this.config.schema, this.config.cronMonitorIntervalSeconds)

      if (!this.stopped) {
        const { rows } = await this.db.executeSql(sql)

        if (!this.stopped && rows.length === 1) {
          await this.cron()
        }
      }
    } catch (err) {
      this.emit(this.events.error, err)
    } finally {
      this.timekeeping = false
    }
  }

  async cron () {
    const schedules = await this.getSchedules()

    const scheduled: ForwardedJob[] = []
    const stillBroken = new Set<string>()

    // Rows whose stored kind disagrees with the expression on them, as found out by reading the
    // expression the other way. Relabelled once the pass has sent what it owes.
    const relabelled: Array<Pick<types.Schedule, 'name' | 'key' | 'kind'>> = []

    // One instant for the whole pass, so every schedule is judged against the same clock and the
    // throttle slot of a forwarded job is measured from the same place its occurrence was.
    const databaseTime = this.databaseNow()

    for (const { name, key, data, options, kind, cron, timezone } of schedules) {
      let due: DueOccurrences

      try {
        due = this.dueOccurrences(cron, kind, timezone, databaseTime)
      } catch (err) {
        // Evaluating one row must not decide the fate of the others. schedule() now rejects an
        // unusable time zone, but a row written by an earlier release — or straight into the table —
        // still throws here. This was a single filter() over every schedule, so one such row
        // propagated out of cron() and silently stopped scheduling for every queue in the
        // deployment, on every pass, until someone found the row. Skip it and warn instead, naming
        // the schedule so it is actually fixable.
        const warned = JSON.stringify([name, key, cron, timezone])

        stillBroken.add(warned)

        if (!this.warnedSchedules.has(warned)) {
          await emitAndPersistWarning(
            this.warningContext,
            WARNING_TYPES.INVALID_SCHEDULE,
            `Warning: schedule for queue "${name}" (key "${key}") could not be evaluated and was skipped: ${(err as Error).message}`,
            { queue: name, key, cron, timezone }
          )
        }

        continue
      }

      if (due.kind !== kind) {
        relabelled.push({ name, key, kind: due.kind })
      }

      const forwarded = { data: { name, data, options }, singletonKey: `${name}__${key}` }

      // A recurrence rule can put an occurrence anywhere in the minute, and a slot measured from
      // insert time would then straddle it: two passes on either side of a slot boundary both find
      // the occurrence inside the window and file it in a slot of their own, sending it twice. So a
      // rule occurrence names the slot it falls in outright. An offset from the insert's own now()
      // would not pin it: everything between reading the clock here and the insert committing
      // counts towards the shifted instant, which lands in the next slot whenever that adds up to a
      // boundary crossing.
      //
      // One job per slot rather than one per occurrence, which is the resolution the docs promise:
      // a rule finer than a slot sends a job a slot, and two occurrences inside one window that
      // fall in slots of their own each send.
      if (due.kind === plans.SCHEDULE_KINDS.rrule) {
        for (const slot of new Set(due.occurrences.map(throttleSlot))) {
          scheduled.push({ ...forwarded, __singletonSlot: slot })
        }
      } else if (due.occurrences.length > 0) {
        // A cron occurrence keeps the slot every release has always filed it in, since an instance
        // still running an older one during a rolling upgrade computes that slot and nothing else,
        // and a slot the two disagree on collapses nothing.
        scheduled.push({ ...forwarded, singletonSeconds: OCCURRENCE_WINDOW_SECONDS })
      }
    }

    this.warnedSchedules = stillBroken

    if (scheduled.length > 0 && !this.stopped) {
      await this.manager.insert(QUEUES.SEND_IT, scheduled)
    }

    // After the sends, so a failed relabel cannot cost an occurrence. Nothing depends on the write:
    // the fallback in dueOccurrences fires the row either way. What it buys is getSchedules() no
    // longer reporting a format the expression is not in, and the row leaving that fallback path.
    if (relabelled.length > 0 && !this.stopped) {
      await this.db.executeSql(plans.setScheduleKinds(this.config.schema), [JSON.stringify(relabelled)])
    }
  }

  shouldSendIt (expression: string, tz: string, kind: types.ScheduleKind = plans.SCHEDULE_KINDS.cron) {
    return this.dueOccurrences(expression, kind, tz).occurrences.length > 0
  }

  /** The database's clock, as this instance last measured it. */
  private databaseNow (): number {
    return Date.now() + this.clockSkew
  }

  /**
   * The occurrences a schedule has come due for, and the format they were read in.
   *
   * `kind` says how to read the expression, and comes off the schedule row: the format was settled
   * when the schedule was written, so a pass reads the expression the one way its author meant it
   * rather than guessing again every 30 seconds.
   *
   * The column is a hint rather than a verdict, though, because two ordinary upgrade paths leave it
   * disagreeing with the expression beside it. A 12.30.x instance's `schedule()` does not name the
   * column, so an upsert from one during a rolling upgrade replaces the expression and leaves
   * whatever kind a newer instance last wrote; a v41 rollback drops the column, and the re-upgrade
   * labels every row from its default. Either way the row reads fine and never fires again. So when
   * an expression cannot be read the way the column says, and is written the other way, it is read
   * the way it is written: one regex, on a path that was already about to give up.
   */
  private dueOccurrences (expression: string, kind: types.ScheduleKind, tz: string, databaseTime = this.databaseNow()): DueOccurrences {
    try {
      return { kind, occurrences: this.readOccurrences(expression, kind, tz, databaseTime) }
    } catch (err) {
      const detected: types.ScheduleKind = isRrule(expression) ? plans.SCHEDULE_KINDS.rrule : plans.SCHEDULE_KINDS.cron

      // The column and the expression agree, so the expression itself is what is wrong with the row,
      // and the caller names it in a warning.
      if (detected === kind) {
        throw err
      }

      return { kind: detected, occurrences: this.readOccurrences(expression, detected, tz, databaseTime) }
    }
  }

  /**
   * Every occurrence of an expression inside the due window, read as `kind` says to read it.
   *
   * Due means "an occurrence in the last minute", whatever the pass interval: a pass runs every
   * `cronMonitorIntervalSeconds` (30 by default), so the window has to be wide enough that an
   * occurrence is still due when the next pass reaches it, and the throttle slot of the forwarded
   * job is what keeps the passes that follow from sending it a second time.
   *
   * The window rather than its most recent point, since a rule can put two occurrences inside it
   * and a read that answers with one of them drops the other. A cron expression cannot: its finest
   * resolution is a second, and consecutive occurrences a second apart share a throttle slot, so
   * only the most recent one can produce a job.
   */
  private readOccurrences (expression: string, kind: types.ScheduleKind, tz: string, databaseTime: number): Date[] {
    const window = new Date(databaseTime - OCCURRENCE_WINDOW_SECONDS * 1000)

    if (kind === plans.SCHEDULE_KINDS.rrule) {
      return occurrencesInWindow(expression, window, new Date(databaseTime), tz)
    }

    const interval = CronExpressionParser.parse(expression, { tz, strict: false, currentDate: new Date(databaseTime) })

    const previous = interval.prev().toDate()

    return previous.getTime() > window.getTime() ? [previous] : []
  }

  private async onSendIt (jobs: types.Job<types.Request>[]): Promise<void> {
    const results = await Promise.allSettled(jobs.map(({ data }) => this.manager.send(data)))

    // Surface any failed forward so a lost cron tick isn't silent
    for (const result of results) {
      if (result.status === 'rejected') {
        this.emit(this.events.error, result.reason)
      }
    }
  }

  async getSchedules (name?: string, key?: string): Promise<types.Schedule[]> {
    let sql = plans.getSchedules(this.config.schema)
    let params: unknown[] = []

    if (name && key !== undefined) {
      sql = plans.getSchedulesByQueueAndKey(this.config.schema)
      params = [name, key]
    } else if (name) {
      sql = plans.getSchedulesByQueue(this.config.schema)
      params = [name]
    }

    const { rows } = await this.db.executeSql(sql, params)

    return rows
  }

  async schedule (name: string, cron: string, data?: unknown, options: types.ScheduleOptions = {}): Promise<void> {
    const { tz = 'UTC', key = '', ...rest } = options

    // The one place the format of an expression is decided. Every reader takes it from the stored
    // kind instead, so a schedule cannot be validated as one format and later evaluated as the
    // other, and a row can say what it is without anyone parsing it.
    const kind: types.ScheduleKind = isRrule(cron) ? plans.SCHEDULE_KINDS.rrule : plans.SCHEDULE_KINDS.cron

    if (kind === plans.SCHEDULE_KINDS.rrule) {
      // From the database's clock, since that is the one the pass reads: a rule whose last
      // occurrence falls inside the skew window is judged here the way it will be evaluated there.
      assertRrule(cron, tz, new Date(this.databaseNow()))
    } else {
      // Expression first, so a bad expression reports as one rather than as a time zone problem. The
      // check is deliberately run against UTC rather than the supplied tz: it only works today
      // because cron-parser is lazy about an unusable zone, and if that ever changes this call would
      // throw the opaque "CronDate: unhandled timestamp" that assertTimezone exists to replace.
      CronExpressionParser.parse(cron, { tz: 'UTC', strict: false })
      assertTimezone(tz)
    }

    Attorney.checkSendArgs([name, data, { ...rest }])
    Attorney.assertKey(key)

    try {
      const sql = plans.schedule(this.config.schema)
      await this.db.executeSql(sql, [name, key, kind, cron, tz, data, options])
    } catch (err: any) {
      if (err.message.includes('foreign key')) {
        err.message = `Queue ${name} not found`
      }

      throw err
    }
  }

  async unschedule (name: string, key = ''): Promise<void> {
    const sql = plans.unschedule(this.config.schema)
    await this.db.executeSql(sql, [name, key])
  }
}

export default Timekeeper
