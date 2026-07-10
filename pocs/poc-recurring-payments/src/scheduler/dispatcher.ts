import { Connection, Client } from '@temporalio/client';
import { pool } from '../db/pool';

const BATCH_SIZE = 10;
const POLL_INTERVAL_MS = 3000;

async function dispatch() {
  const connection = await Connection.connect({ address: 'localhost:7233' });
  const client = new Client({ connection });

  console.log('⏰ Scheduler started — polling every 3s');

  while (true) {
    try {
      // Claim due rows with subscription data (JOIN to get real amount/destinationId)
      const res = await pool.query(
        `UPDATE payment_execution_queue
         SET status = 'PROCESSING', locked_at = now(), locked_by = 'scheduler-1', updated_at = now()
         WHERE id IN (
           SELECT id FROM payment_execution_queue
           WHERE status = 'READY' AND due_at <= now()
           ORDER BY due_at
           LIMIT $1
         )
         RETURNING *`,
        [BATCH_SIZE]
      );

      if (res.rows.length > 0) {
        console.log(`📋 Claimed ${res.rows.length} executions`);
      }

      // Fetch subscription details for claimed rows
      const subIds = res.rows.map(r => r.subscription_id);
      const subsResult = subIds.length > 0
        ? await pool.query(
            `SELECT id, user_id, destination_id, amount, max_retries FROM subscriptions WHERE id = ANY($1)`,
            [subIds]
          )
        : { rows: [] };
      const subsMap = new Map(subsResult.rows.map((s: any) => [s.id, s]));

      for (const row of res.rows) {
        const sub = subsMap.get(row.subscription_id) || { user_id: 'unknown', destination_id: 'unknown', amount: 0, max_retries: 3 };
        const workflowId = `recurring-${row.subscription_id}-${row.due_at.toISOString().split('T')[0]}`;

        try {
          await client.workflow.start('recurringPaymentWorkflow', {
            args: [{
              subscriptionId: row.subscription_id,
              subscriptionType: row.subscription_type,
              executionDate: row.due_at.toISOString().split('T')[0],
              destinationId: sub.destination_id,
              amount: parseFloat(sub.amount),
              maxRetries: sub.max_retries,
              userId: sub.user_id,
              eventConfig: { enabled: true },
              metadata: {},
            }],
            taskQueue: 'payments-platform',
            workflowId,
            workflowExecutionTimeout: '4 days', // Max time for parent (3 retries × 1 day + buffer)
          });
          console.log(`  ✅ Started workflow: ${workflowId}`);

          // Record workflow ID
          await pool.query(
            `UPDATE payment_execution_queue SET workflow_id = $1 WHERE id = $2`,
            [workflowId, row.id]
          );
        } catch (err: any) {
          if (err.message?.includes('already started') || err.code === 'ALREADY_EXISTS') {
            console.log(`  ⚠️ Workflow already exists: ${workflowId} (idempotent skip)`);
          } else {
            console.error(`  ❌ Failed to start workflow: ${err.message}`);
            // Release the row back to READY so it can be retried
            await pool.query(
              `UPDATE payment_execution_queue SET status = 'READY', locked_at = NULL, locked_by = NULL WHERE id = $1`,
              [row.id]
            );
          }
        }
      }

      // Recovery: unlock rows stuck in PROCESSING for more than 5 minutes (scheduler crash recovery)
      const recovered = await pool.query(
        `UPDATE payment_execution_queue
         SET status = 'READY', locked_at = NULL, locked_by = NULL, updated_at = now()
         WHERE status = 'PROCESSING' AND locked_at < now() - INTERVAL '5 minutes'
         RETURNING id`
      );
      if (recovered.rows.length > 0) {
        console.log(`🔄 Recovered ${recovered.rows.length} stuck executions`);
      }
    } catch (err) {
      console.error('Scheduler error:', err);
    }

    await new Promise(resolve => setTimeout(resolve, POLL_INTERVAL_MS));
  }
}

dispatch().catch(console.error);
