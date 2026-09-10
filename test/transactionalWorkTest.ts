import { expect } from 'vitest'
import * as helper from './testHelper.ts'
import { PgBoss } from '../src/index.ts'
import type * as types from '../src/types.ts'
import * as plans from '../src/plans.ts'
import { delay } from '../src/tools.ts'
import { ctx } from './hooks.ts'

// PGlite is a single in-process connection supplied as a `db` adapter, which has no
// beginTransaction, so transactional workers are unavailable there by design.
const describeTransactional = helper.describePglite

// Waits for a condition the worker satisfies asynchronously, rather than sleeping a fixed budget.
async function until (check: () => Promise<boolean>, timeoutMs = 8000): Promise<void> {
  const deadline = Date.now() + timeoutMs

  while (Date.now() < deadline) {
    if (await check()) return
    await delay(50)
  }

  throw new Error('condition was not met in time')
}

describeTransactional('transactional work', function () {
  it('should commit handler writes with the job completion', async function () {
    ctx.boss = await helper.start(ctx.bossConfig)

    const sideEffects = `${ctx.schema}.side_effect`
    const db = ctx.boss.getDb()

    await db.executeSql(`CREATE TABLE ${sideEffects} (job_id uuid primary key)`)

    const jobId = await ctx.boss.send(ctx.schema, { work: true })
    helper.assertTruthy(jobId)

    await ctx.boss.work(ctx.schema, { transactional: true }, async (jobs, tx) => {
      await tx.executeSql(`INSERT INTO ${sideEffects} (job_id) VALUES ($1)`, [jobs[0].id])
    })

    await until(async () => {
      const job = await ctx.boss!.getJobById(ctx.schema, jobId)
      return job?.state === 'completed'
    })

    const { rows } = await db.executeSql(`SELECT job_id FROM ${sideEffects}`)

    expect(rows.length).toBe(1)
    expect(rows[0].job_id).toBe(jobId)
  })

  it('should roll handler writes back when the handler throws', async function () {
    ctx.boss = await helper.start(ctx.bossConfig)

    const sideEffects = `${ctx.schema}.side_effect`
    const db = ctx.boss.getDb()

    await db.executeSql(`CREATE TABLE ${sideEffects} (job_id uuid primary key)`)

    const jobId = await ctx.boss.send(ctx.schema, { work: true }, { retryLimit: 0 })
    helper.assertTruthy(jobId)

    await ctx.boss.work(ctx.schema, { transactional: true }, async (jobs, tx) => {
      await tx.executeSql(`INSERT INTO ${sideEffects} (job_id) VALUES ($1)`, [jobs[0].id])
      throw new Error('handler exploded')
    })

    await until(async () => {
      const job = await ctx.boss!.getJobById(ctx.schema, jobId)
      return job?.state === 'failed'
    })

    const { rows } = await db.executeSql(`SELECT job_id FROM ${sideEffects}`)

    expect(rows.length).toBe(0)
  })

  it('should leave the job active and readable outside the transaction while the handler runs', async function () {
    ctx.boss = await helper.start(ctx.bossConfig)

    const jobId = await ctx.boss.send(ctx.schema, { work: true })
    helper.assertTruthy(jobId)

    let stateDuringHandler: string | undefined

    // The claim is committed before the transaction opens, which is what leaves every supervision
    // path (timeouts, heartbeats, another instance's monitor) able to see the job it is holding.
    await ctx.boss.work(ctx.schema, { transactional: true }, async () => {
      const job = await ctx.boss!.getJobById(ctx.schema, jobId)
      stateDuringHandler = job?.state
    })

    await until(async () => {
      const job = await ctx.boss!.getJobById(ctx.schema, jobId)
      return job?.state === 'completed'
    })

    expect(stateDuringHandler).toBe('active')
  })

  it('should still apply retry accounting after a rollback', async function () {
    ctx.boss = await helper.start(ctx.bossConfig)

    const jobId = await ctx.boss.send(ctx.schema, { work: true }, { retryLimit: 1, retryDelay: 0 })
    helper.assertTruthy(jobId)

    let attempts = 0

    await ctx.boss.work(ctx.schema, { transactional: true, pollingIntervalSeconds: 0.5 }, async () => {
      attempts++
      throw new Error('handler exploded')
    })

    await until(async () => {
      const job = await ctx.boss!.getJobById(ctx.schema, jobId)
      return job?.state === 'failed'
    })

    // one retry, then terminal: the rollback takes the handler's writes and nothing else, so the
    // attempt the fetch recorded still counts
    expect(attempts).toBe(2)

    const job = await ctx.boss.getJobById(ctx.schema, jobId)
    helper.assertTruthy(job)
    expect(job.retryCount).toBe(1)
  })

  it('should dead letter a transactional job whose retries run out', async function () {
    const deadLetter = `${ctx.schema}_dlq`

    // noDefault so the source queue is created here, with its dead letter queue attached: the
    // default helper queue already exists without one, and createQueue would not add it.
    ctx.boss = await helper.start({ ...ctx.bossConfig, noDefault: true })

    await ctx.boss.createQueue(deadLetter)
    await ctx.boss.createQueue(ctx.schema, { deadLetter })

    const jobId = await ctx.boss.send(ctx.schema, { work: true }, { retryLimit: 0 })
    helper.assertTruthy(jobId)

    await ctx.boss.work(ctx.schema, { transactional: true }, async () => {
      throw new Error('handler exploded')
    })

    await until(async () => {
      const [job] = await ctx.boss!.fetch(deadLetter)
      return !!job
    })
  })

  it('should let the supervisor reclaim a transactional job whose handler never returns', async function () {
    ctx.boss = await helper.start({ ...ctx.bossConfig, noDefault: true })

    await ctx.boss.createQueue(ctx.schema, { retryLimit: 0 })

    const jobId = await ctx.boss.send(ctx.schema, { work: true })
    helper.assertTruthy(jobId)

    let releaseHandler = () => {}
    let handlerStarted = false

    await ctx.boss.work(ctx.schema, { transactional: true }, async () => {
      handlerStarted = true
      await new Promise<void>(resolve => { releaseHandler = resolve })
    })

    await until(async () => handlerStarted)

    // Backdate the claim past its expiration, the way a process that died holding the transaction
    // would look to the next supervise pass.
    const db = await helper.getDb()

    try {
      await db.executeSql(`UPDATE ${ctx.schema}.job SET started_on = now() - interval '1 hour' WHERE id = $1`, [jobId])
    } finally {
      await db.close()
    }

    await ctx.boss.supervise(ctx.schema)

    const job = await ctx.boss.getJobById(ctx.schema, jobId)
    helper.assertTruthy(job)
    expect(job.state).toBe('failed')
    expect(job.output).toEqual({ value: { message: 'job timed out' } })

    releaseHandler()
  })

  it('should refresh the heartbeat while a transactional handler runs', async function () {
    ctx.boss = await helper.start({ ...ctx.bossConfig, noDefault: true })

    await ctx.boss.createQueue(ctx.schema, { heartbeatSeconds: 10 })

    const jobId = await ctx.boss.send(ctx.schema, { work: true })
    helper.assertTruthy(jobId)

    const db = await helper.getDb()

    const readHeartbeat = async () => {
      const { rows } = await db.executeSql(`SELECT heartbeat_on FROM ${ctx.schema}.job WHERE id = $1`, [jobId])
      return rows[0].heartbeat_on.getTime() as number
    }

    let refreshed = false

    // The heartbeat runs on a pooled connection against the claimed row, so it reaches the job a
    // transactional handler is holding just as it does any other.
    await ctx.boss.work(ctx.schema, { transactional: true, heartbeatRefreshSeconds: 0.5 }, async () => {
      const before = await readHeartbeat()
      await until(async () => (await readHeartbeat()) > before)
      refreshed = true
    })

    await until(async () => {
      const job = await ctx.boss!.getJobById(ctx.schema, jobId)
      return job?.state === 'completed'
    })

    await db.close()

    expect(refreshed).toBe(true)
  })

  it('should work with localGroupConcurrency', async function () {
    ctx.boss = await helper.start(ctx.bossConfig)

    const ids: string[] = []

    for (let i = 0; i < 2; i++) {
      const id: string | null = await ctx.boss.send(ctx.schema, { seq: i }, { group: { id: 'tx-group' } })
      helper.assertTruthy(id)
      ids.push(id)
    }

    await ctx.boss.work(ctx.schema, { transactional: true, localGroupConcurrency: 1, pollingIntervalSeconds: 0.5 }, async () => {})

    await until(async () => {
      const jobs = await Promise.all(ids.map(id => ctx.boss!.getJobById(ctx.schema, id)))
      return jobs.every(job => job?.state === 'completed')
    })
  })

  it('should work with groupConcurrency', async function () {
    ctx.boss = await helper.start(ctx.bossConfig)

    const ids: string[] = []

    for (let i = 0; i < 2; i++) {
      const id: string | null = await ctx.boss.send(ctx.schema, { seq: i }, { group: { id: 'tx-group' } })
      helper.assertTruthy(id)
      ids.push(id)
    }

    await ctx.boss.work(ctx.schema, { transactional: true, groupConcurrency: 1, pollingIntervalSeconds: 0.5 }, async () => {})

    await until(async () => {
      const jobs = await Promise.all(ids.map(id => ctx.boss!.getJobById(ctx.schema, id)))
      return jobs.every(job => job?.state === 'completed')
    })
  })

  it('should let the handler complete a job itself through the transaction', async function () {
    ctx.boss = await helper.start(ctx.bossConfig)

    const jobId = await ctx.boss.send(ctx.schema, { work: true })
    helper.assertTruthy(jobId)

    await ctx.boss.work(ctx.schema, { transactional: true }, async (jobs, tx) => {
      await ctx.boss!.complete(ctx.schema, jobs[0].id, { settledBy: 'handler' }, { db: tx })
    })

    await until(async () => {
      const job = await ctx.boss!.getJobById(ctx.schema, jobId)
      return job?.state === 'completed'
    })

    const job = await ctx.boss.getJobById<object>(ctx.schema, jobId)
    helper.assertTruthy(job)
    expect(job.output).toEqual({ settledBy: 'handler' })
  })

  it('should process a batch as one unit', async function () {
    ctx.boss = await helper.start(ctx.bossConfig)

    const ids: string[] = []

    for (let i = 0; i < 3; i++) {
      const id: string | null = await ctx.boss.send(ctx.schema, { seq: i })
      helper.assertTruthy(id)
      ids.push(id)
    }

    let seen = 0

    await ctx.boss.work(ctx.schema, { transactional: true, batchSize: 3 }, async (jobs) => {
      seen = jobs.length
    })

    await until(async () => {
      const jobs = await Promise.all(ids.map(id => ctx.boss!.getJobById(ctx.schema, id)))
      return jobs.every(job => job?.state === 'completed')
    })

    expect(seen).toBe(3)
  })

  it('should roll back a handler abandoned by a shutdown', async function () {
    ctx.boss = await helper.start(ctx.bossConfig)

    const sideEffects = `${ctx.schema}.side_effect`
    const db = ctx.boss.getDb()

    await db.executeSql(`CREATE TABLE ${sideEffects} (note text)`)

    const jobId = await ctx.boss.send(ctx.schema, { work: true }, { retryLimit: 0 })
    helper.assertTruthy(jobId)

    let handlerTx: types.IDatabase | undefined
    let firstHalfWritten = false

    // Half the handler's work is in the database and the rest never runs, which is what a
    // non-graceful stop does to any handler it catches mid-flight.
    await ctx.boss.work(ctx.schema, { transactional: true }, async (jobs, tx) => {
      handlerTx = tx
      await tx.executeSql(`INSERT INTO ${sideEffects} (note) VALUES ('first-half')`)
      firstHalfWritten = true
      await delay(1000)
      await tx.executeSql(`INSERT INTO ${sideEffects} (note) VALUES ('second-half')`)
    })

    await until(async () => firstHalfWritten)

    await ctx.boss.stop({ graceful: false, close: false })

    // The transaction refusing statements is the worker having settled it, which is what the
    // assertions below are waiting on: an uncommitted insert is invisible from another connection
    // either way, so an empty table only means anything once the transaction is over.
    await until(async () => {
      try {
        await handlerTx!.executeSql('SELECT 1')
        return false
      } catch {
        return true
      }
    })

    const { rows } = await db.executeSql(`SELECT note FROM ${sideEffects}`)

    expect(rows.length).toBe(0)

    const job = await ctx.boss.getJobById(ctx.schema, jobId)
    helper.assertTruthy(job)
    expect(job.state).toBe('failed')
  })

  it('should roll back when the claim is lost while the handler runs', async function () {
    ctx.boss = await helper.start(ctx.bossConfig)

    const ledger = `${ctx.schema}.ledger`
    const db = ctx.boss.getDb()

    await db.executeSql(`CREATE TABLE ${ledger} (id serial primary key)`)

    const jobId = await ctx.boss.send(ctx.schema, { work: true }, { retryLimit: 5, retryDelay: 0 })
    helper.assertTruthy(jobId)

    let attempts = 0
    let stolen = false

    await ctx.boss.work(ctx.schema, { transactional: true, pollingIntervalSeconds: 0.5 }, async (jobs, tx) => {
      attempts++
      await tx.executeSql(`INSERT INTO ${ledger} DEFAULT VALUES`)

      if (!stolen) {
        stolen = true
        // On a pooled connection, so it is the job being taken away from this handler rather than
        // the handler settling it: an operator's fail(), a heartbeat the database stopped seeing,
        // expireInSeconds, another instance's supervisor. Committing here would leave the ledger
        // row under a job that is about to run again.
        await ctx.boss!.fail(ctx.schema, jobs[0].id, new Error('stolen'))
      }
    })

    await until(async () => {
      const job = await ctx.boss!.getJobById(ctx.schema, jobId)
      return job?.state === 'completed'
    })

    const { rows } = await db.executeSql(`SELECT id FROM ${ledger}`)

    // One row for two attempts: the stolen one rolled back, the one that kept its claim committed.
    expect(attempts).toBe(2)
    expect(rows.length).toBe(1)
  })

  it('should say so when the handler swallows a SQL error and leaves the transaction aborted', async function () {
    ctx.boss = await helper.start(ctx.bossConfig)

    const jobId = await ctx.boss.send(ctx.schema, { work: true }, { retryLimit: 0 })
    helper.assertTruthy(jobId)

    await ctx.boss.work(ctx.schema, { transactional: true }, async (jobs, tx) => {
      try {
        await tx.executeSql('SELECT 1 FROM a_table_that_does_not_exist')
      } catch {
        // swallowed on purpose: the transaction stays aborted, so pg-boss's own completion is the
        // statement that trips over it
      }
    })

    await until(async () => {
      const job = await ctx.boss!.getJobById(ctx.schema, jobId)
      return job?.state === 'failed'
    })

    const job = await ctx.boss.getJobById(ctx.schema, jobId)
    helper.assertTruthy(job)
    expect(JSON.stringify(job.output)).toContain('left its transaction aborted')
  })

  it('should warn when transactional workers leave the pool no headroom', async function () {
    ctx.boss = await helper.start({ ...ctx.bossConfig, max: 2 })

    const warnings: any[] = []
    ctx.boss.on('warning', warning => warnings.push(warning))

    const workerId = await ctx.boss.work(ctx.schema, { transactional: true, localConcurrency: 2 }, async () => {})

    await ctx.boss.offWork(ctx.schema, { id: workerId })

    expect(warnings.some(w => w.data?.type === 'transactional_pool_headroom')).toBe(true)
  })

  it('should persist the pool headroom warning under persistWarnings', async function () {
    ctx.boss = await helper.start({ ...ctx.bossConfig, max: 2, persistWarnings: true })

    const workerId = await ctx.boss.work(ctx.schema, { transactional: true, localConcurrency: 2 }, async () => {})

    await ctx.boss.offWork(ctx.schema, { id: workerId })

    const db = await helper.getDb()

    try {
      const { rows } = await db.executeSql(plans.getWarnings(ctx.schema), [null, 10, 0])
      expect(rows.some(row => row.type === 'transactional_pool_headroom')).toBe(true)
    } finally {
      await db.close()
    }
  })

  it('should reject a transactional worker on a db without transaction support', async function () {
    ctx.boss = await helper.start(ctx.bossConfig)

    // an adapter-style db exposing only executeSql, which is the documented minimum
    const inner = ctx.boss.getDb()
    const bare = { executeSql: (text: string, values?: unknown[]) => inner.executeSql(text, values) }

    const boss2 = new PgBoss({ ...ctx.bossConfig, db: bare, createSchema: false, migrate: false })

    await boss2.start()

    try {
      await expect(async () => {
        await boss2.work(ctx.schema, { transactional: true }, async () => {})
      }).rejects.toThrow(/beginTransaction/)
    } finally {
      await boss2.stop({ graceful: false })
    }
  })
})

describeTransactional('transaction handle', function () {
  it('should settle once and refuse anything after', async function () {
    ctx.boss = await helper.start(ctx.bossConfig)

    const db = ctx.boss.getDb()
    const tx = await db.beginTransaction!()

    await tx.db.executeSql('SELECT 1')
    await tx.rollback()

    // idempotent: a second rollback must not send ROLLBACK down a connection the pool has since
    // handed to someone else
    await tx.rollback()

    await expect(async () => await tx.db.executeSql('SELECT 1')).rejects.toThrow(/already settled/)
    await expect(async () => await tx.commit()).rejects.toThrow(/already settled/)
  })
})

helper.describeMultiConnectionOnly('transaction handle (connection loss)', function () {
  it('should survive a connection dropped mid-transaction', async function () {
    ctx.boss = await helper.start(ctx.bossConfig)

    const db = ctx.boss.getDb()
    const tx = await db.beginTransaction!()

    const { rows } = await tx.db.executeSql('SELECT pg_backend_pid() AS pid')
    const other = await helper.getDb()

    try {
      await other.executeSql('SELECT pg_terminate_backend($1)', [rows[0].pid])
    } finally {
      await other.close()
    }

    // pg-pool takes its own 'error' listener off a checked-out client, so without one of its own
    // the handle would let this drop end the process instead of failing the transaction.
    await until(async () => {
      try {
        await tx.db.executeSql('SELECT 1')
        return false
      } catch {
        return true
      }
    })

    await tx.rollback()
  })
})

// Option validation needs no database transaction support, so it runs on every backend.
describe('transactional work options', function () {
  it('should reject a non-boolean transactional option', async function () {
    ctx.boss = await helper.start(ctx.bossConfig)

    await expect(async () => {
      // @ts-expect-error deliberately passing the wrong type
      await ctx.boss.work(ctx.schema, { transactional: 'yes' }, async () => {})
    }).rejects.toThrow(/transactional must be a boolean/)
  })

  it('should reject transactional combined with perJobResults', async function () {
    ctx.boss = await helper.start(ctx.bossConfig)

    await expect(async () => {
      await ctx.boss!.work(ctx.schema, { transactional: true, perJobResults: true }, async () => [])
    }).rejects.toThrow(/perJobResults/)
  })
})

// The two ways a pg-boss-owned transaction fails on a statement of its own rather than the
// caller's: the BEGIN that opens it, and the COMMIT that settles it. Both have to release the
// client with the error, so the pool discards a connection whose transaction state it cannot know
// instead of handing it to the next caller.
describeTransactional('transaction handle (settle failures)', function () {
  it('should release the connection when the transaction cannot be opened', async function () {
    const db = await helper.getDb()
    const pool = (db as any).pool
    const released: Array<Error | undefined> = []

    // A client that answers everything except BEGIN, which is how a connection that died while
    // idle in the pool behaves: the checkout succeeds, and the first statement is what finds out.
    const client = {
      query: async (text: string) => {
        if (text === 'BEGIN') throw new Error('connection is dead')
        return { rows: [] }
      },
      on: () => {},
      removeListener: () => {},
      release: (err?: Error) => released.push(err)
    }

    try {
      ;(db as any).pool = { connect: async () => client }

      await expect(async () => await db.beginTransaction()).rejects.toThrow('connection is dead')

      expect(released).toHaveLength(1)
      expect(released[0]).toBeInstanceOf(Error)
    } finally {
      ;(db as any).pool = pool
      await db.close()
    }
  })

  // A deferred constraint is the one thing that makes a COMMIT fail after every statement inside
  // the transaction succeeded. CockroachDB has neither temp tables nor deferrable unique
  // constraints without an experimental flag, hence postgres only.
  helper.itPostgresOnly('should reject and settle the handle when the commit fails', async function () {
    ctx.boss = await helper.start(ctx.bossConfig)

    const db = ctx.boss.getDb()
    const tx = await db.beginTransaction!()

    await tx.db.executeSql('CREATE TEMP TABLE commit_check (id int UNIQUE DEFERRABLE INITIALLY DEFERRED)')
    await tx.db.executeSql('INSERT INTO commit_check (id) VALUES (1), (1)')

    await expect(async () => await tx.commit()).rejects.toThrow(/duplicate key/)

    // Released with the error, so the handle is settled and the connection is gone with the
    // transaction it could not commit.
    await expect(async () => await tx.db.executeSql('SELECT 1')).rejects.toThrow(/already settled/)
  })
})
