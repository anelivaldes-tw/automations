import express from 'express';
import { pool } from '../db/pool';

const app = express();
app.use(express.json());

// POST /subscriptions — Create a recurring payment subscription
app.post('/subscriptions', async (req, res) => {
  const { userId, subscriptionType, destinationId, amount, frequency } = req.body;

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // Insert subscription
    const subResult = await client.query(
      `INSERT INTO subscriptions (user_id, subscription_type, destination_id, amount, frequency, next_execution_at)
       VALUES ($1, $2, $3, $4, $5, now() + INTERVAL '10 seconds')
       RETURNING id, next_execution_at`,
      [userId || 'user-001', subscriptionType || 'BILL', destinationId || 'biller-001', amount || 50.00, frequency || 'DAILY']
    );

    const sub = subResult.rows[0];

    // Enqueue first execution
    await client.query(
      `INSERT INTO payment_execution_queue (subscription_id, subscription_type, due_at)
       VALUES ($1, $2, $3)`,
      [sub.id, subscriptionType || 'BILL', sub.next_execution_at]
    );

    // Write reminder to outbox
    await client.query(
      `INSERT INTO notification_outbox (subscription_id, event_type, delivery_class, payload, idempotency_key, scheduled_for)
       VALUES ($1, 'REMINDER', 'DELAYED', $2, $3, $4)`,
      [
        sub.id,
        JSON.stringify({ message: 'Your automatic payment will execute soon' }),
        `${sub.id}-reminder-first`,
        sub.next_execution_at,
      ]
    );

    await client.query('COMMIT');

    console.log(`✅ Subscription created: ${sub.id} (type: ${subscriptionType || 'BILL'})`);
    res.json({ id: sub.id, nextExecution: sub.next_execution_at });
  } catch (err: any) {
    await client.query('ROLLBACK');
    console.error('Error creating subscription:', err);
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

// PATCH /subscriptions/:id/cancel — Cancel a subscription (also cancels running workflow)
app.patch('/subscriptions/:id/cancel', async (req, res) => {
  const { id } = req.params;

  try {
    // Update DB status
    await pool.query(
      `UPDATE subscriptions SET status = 'INACTIVE', updated_at = now() WHERE id = $1`,
      [id]
    );

    // Cancel any running workflow for this subscription via Temporal
    const { Connection, Client } = await import('@temporalio/client');
    const connection = await Connection.connect({ address: 'localhost:7233' });
    const client = new Client({ connection });

    // Find and cancel active workflows for this subscription
    const today = new Date().toISOString().split('T')[0];
    const workflowId = `recurring-${id}-${today}`;
    try {
      const handle = client.workflow.getHandle(workflowId);
      await handle.cancel();
      console.log(`🚫 Cancelled workflow: ${workflowId}`);
    } catch (err: any) {
      // Workflow may not exist or already completed — that's fine
      console.log(`ℹ️ No active workflow to cancel: ${workflowId}`);
    }

    res.json({ id, status: 'INACTIVE', message: 'Subscription cancelled' });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// GET /subscriptions — List all
app.get('/subscriptions', async (_req, res) => {
  const result = await pool.query('SELECT * FROM subscriptions ORDER BY created_at DESC');
  res.json(result.rows);
});

// GET /outbox — See notification events
app.get('/outbox', async (_req, res) => {
  const result = await pool.query('SELECT * FROM notification_outbox ORDER BY created_at DESC');
  res.json(result.rows);
});

// GET /queue — See execution queue
app.get('/queue', async (_req, res) => {
  const result = await pool.query('SELECT * FROM payment_execution_queue ORDER BY created_at DESC');
  res.json(result.rows);
});

// --- Internal endpoints (called by child workflows in other services) ---

// POST /internal/attempt-failed — Child workflow notifies a failed attempt
app.post('/internal/attempt-failed', async (req, res) => {
  const { subscriptionId, attempt, maxAttempts, nextRetryIn } = req.body;
  const idempotencyKey = `${subscriptionId}-attempt-${attempt}-failed`;

  try {
    await pool.query(
      `INSERT INTO notification_outbox (subscription_id, event_type, delivery_class, payload, idempotency_key)
       VALUES ($1, 'ATTEMPT_FAILED', 'IMMEDIATE', $2, $3)
       ON CONFLICT (idempotency_key) DO NOTHING`,
      [
        subscriptionId,
        JSON.stringify({
          message: `Tu pago falló (intento ${attempt}/${maxAttempts}). Reintentaremos en ${nextRetryIn}.`,
          attempt,
          maxAttempts,
          nextRetryIn,
        }),
        idempotencyKey,
      ]
    );
    console.log(`[internal/attempt-failed] ${subscriptionId} → attempt ${attempt} → outbox written ✅`);
    res.json({ ok: true });
  } catch (err: any) {
    console.error('[internal/attempt-failed] Error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

const PORT = 3000;
app.listen(PORT, () => {
  console.log(`🚀 API server running on http://localhost:${PORT}`);
  console.log(`   POST /subscriptions  — create a subscription`);
  console.log(`   GET  /subscriptions  — list all`);
  console.log(`   GET  /queue          — see execution queue`);
  console.log(`   GET  /outbox         — see notifications`);
});
