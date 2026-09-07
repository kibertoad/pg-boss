# Scheduling

Jobs may be created automatically based on a cron expression or an [RRULE](#rrule-expressions). As with other cron-based systems, at least one instance needs to be running for scheduling to work. In order to reduce the amount of evaluations, schedules are checked every 30 seconds, which means the 6-placeholder format should be discouraged in favor of the minute-level precision 5-placeholder format.

For example, use this format, which implies "any second during 3:30 am every day"

```
30 3 * * *
```

but **not** this format which is parsed as "only run exactly at 3:30:30 am every day"

```
30 30 3 * * *
```

To change how often schedules are checked, you can set `cronMonitorIntervalSeconds`. To change how often cron jobs are run, you can set `cronWorkerIntervalSeconds`.

In order mitigate clock skew and drift, every 10 minutes the clocks of each instance are compared to the database server's clock. The skew, if any, is stored and used as an offset during cron evaluation to ensure all instances are synchronized. Internally, job throttling options are then used to make sure only 1 job is sent even if multiple instances are running.

If needed, the default clock monitoring interval can be adjusted using `clockMonitorIntervalSeconds`. Additionally, to disable scheduling on an instance completely, use the following in the constructor options.

```js
{
  schedule: false
}
```

For more cron documentation and examples see the docs for the [cron-parser package](https://www.npmjs.com/package/cron-parser).

## RRULE expressions

An expression carrying a `FREQ=` part, or a line that opens with an iCalendar property such as `DTSTART` or `RRULE`, is read as a recurrence rule as defined in [RFC 5545](https://datatracker.ietf.org/doc/html/rfc5545#section-3.3.10) and evaluated by [rrule-temporal](https://www.npmjs.com/package/rrule-temporal). Everything else is a cron expression, which cannot be mistaken for a rule since no cron field contains `=`, `:` or `;`.

Rules cover the schedules cron cannot express: the last Friday of the month, every second Monday, a schedule that stops on a date or after a number of runs.

```js
// 5pm on the last Friday of the month, Chicago time
await boss.schedule('report', 'FREQ=MONTHLY;BYDAY=-1FR;BYHOUR=17', null, { tz: 'America/Chicago' })
```

The expression is either the rule on its own, as above, or the recurrence lines of a calendar entry: a `DTSTART` line, the `RRULE` line, and optional `RDATE` and `EXDATE` lines. Paste those lines rather than a whole export, since a `UID`, a `SUMMARY` or the `BEGIN` and `END` lines around them say nothing about when a job should run and are rejected:

```js
await boss.schedule('standup', [
  'DTSTART;TZID=Europe/Berlin:20260901T090000',
  'RRULE:FREQ=WEEKLY;BYDAY=MO,TU,WE,TH,FR',
  'EXDATE;TZID=Europe/Berlin:20261224T090000'
].join('\n'))
```

* **Time zone**

  `DTSTART` decides it when it names one, as `DTSTART;TZID=Europe/Berlin:20260901T090000` does. Otherwise the `tz` option does, so `FREQ=DAILY;BYHOUR=9` with `tz: 'America/Chicago'` runs at nine in Chicago, across daylight saving transitions.

* **DTSTART**

  A rule that carries no `DTSTART` is anchored on 1970-01-01T00:00:00 in the schedule's time zone, the same anchor in every instance and every release. That anchor is what an `INTERVAL` counts from, so `FREQ=HOURLY;INTERVAL=6` runs every six hours from midnight on the epoch. Supply a `DTSTART` to choose the phase yourself. `COUNT` is rejected without one, since counting from the epoch leaves a rule with nothing left to send.

  An `HOURLY`, `MINUTELY` or `SECONDLY` `INTERVAL` counts elapsed time rather than clock time, and the epoch anchor falls in standard time, so in a zone that observes daylight saving the local time of those occurrences moves with the offset: `FREQ=HOURLY;INTERVAL=6` with `tz: 'America/Chicago'` lands on 00:00, 06:00, 12:00 and 18:00 in January and on 01:00, 07:00, 13:00 and 19:00 in July. Name the hours to pin them to the clock instead. `FREQ=DAILY;BYHOUR=0,6,12,18` holds across a transition, as `0 */6 * * *` does.

* **Finite rules**

  `UNTIL` and `COUNT` are honored. Once the last occurrence has passed, the schedule stays in the table and sends nothing further. A rule that has nothing left to send when `schedule()` is called is rejected instead of stored, since a schedule that quietly does nothing is a failure nobody sees.

* **Resolution**

  Schedules are checked every 30 seconds against the minute an occurrence falls in, so a rule finer than a minute, such as `FREQ=SECONDLY` or a `BYSECOND` list, sends at most one job a minute. This is the same limitation the 6-placeholder cron format has, and for the same reason.

A rule is understood by any instance running a release that supports one. During a rolling upgrade an instance still on an older release reads the expression as cron, cannot parse it, and reports an [`invalid_schedule`](./events.md#warning) warning until it is replaced, so rule schedules are best added once the deployment is upgraded.

`schedule()` validates the expression, so a rule that would be read differently than it was meant is rejected before it reaches the table:

* an unknown part such as `BYHOURS=9`, or an unknown property such as `DTSRAT`, which a parser drops before evaluating the rest
* a value out of range, such as `BYHOUR=25` or the `25` in `BYHOUR=9,25`, which a parser drops just as quietly
* a part named twice, such as `BYHOUR=9;BYHOUR=17`, where the second replaces the first rather than widening it
* a second `DTSTART` or `RRULE`
* an `RDATE` or `EXDATE` given as a date where `DTSTART` is a date time, which excludes or adds midnight rather than the occurrence it names
* the combinations RFC 5545 forbids outright, such as `BYMONTHDAY` with a weekly frequency
* a time zone no evaluation can use, reported in the same words a cron schedule reports it in

## Managing schedules

### `schedule(name, cron, data, options)`

Schedules a job to be sent to the specified queue based on a cron expression or an [RRULE](#rrule-expressions). If the schedule already exists, it's updated to the new expression.

**Arguments**

- `name`: string, *required*
- `cron`: string, *required*. A cron expression, or an [RRULE](#rrule-expressions)
- `data`: object
- `options`: object

`options` supports all properties in `send()` as well as the following additional options.

* **tz**

  An optional time zone name. If not specified, the default is UTC. An unrecognized time zone is
  rejected by `schedule()`, so a typo cannot be stored and then fail on the cron pass.

* **key**
  
  An optional unique key if more than schedule is needed for this queue.


For example, the following code will send a job at 3:00am in the US central time zone into the queue `notification-abc`.

```js
await boss.schedule('notification-abc', `0 3 * * *`, null, { tz: 'America/Chicago' })
```

### `unschedule(name)`

Removes all scheduled jobs for the specified queue name.

```js
await boss.unschedule('notification-abc')
```

### `unschedule(name, key)`

Removes a schedule by queue name and unique key.

```js
// create two schedules on the same queue, then remove just one
await boss.schedule('report', '0 6 * * *', { region: 'us' }, { key: 'us' })
await boss.schedule('report', '0 18 * * *', { region: 'eu' }, { key: 'eu' })

await boss.unschedule('report', 'eu')
```

### `getSchedules()`

Returns all scheduled jobs.

```js
const schedules = await boss.getSchedules()

for (const schedule of schedules) {
  console.log(`${schedule.name} (${schedule.key}): ${schedule.cron} ${schedule.timezone}`)
}
```

### `getSchedules(name)`

Returns all scheduled jobs by queue name.

```js
const schedules = await boss.getSchedules('report')
```

### `getSchedules(name, key)`

Returns all scheduled jobs by queue name and unique key.

```js
const [schedule] = await boss.getSchedules('report', 'eu')
```