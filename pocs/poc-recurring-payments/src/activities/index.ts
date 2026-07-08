import { pool } from '../db/pool';

/**
 * Activity 1: Validate that subscription is still active
 */
export async function validateSubscription(subscriptionId: string): Promise<boolean> {
  const res = await pool.query(
    `SELECT status FROM subscriptions WHERE id = $1`,
    [subscriptionId]
  );
  if (res.rows.length === 0) return false;
  const isActive = res.rows[0].status === 'ACTIVE';
  console.log(`[validateSubscription] ${subscriptionId} → ${isActive ? 'ACTIVE' : 'INACTIVE'}`);
  return isActive;
}

/**
 * Activity 3: Record payment result (atomic TX)
 * Also re-enqueues next execution if successful (truly recurring)
 */
export async function recordPaymentResult(input: {
  subscriptionId: string;
  executionDate: string;
  result: string;
  attemptCount: number;
}): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // Update subscription state
    if (input.result === 'SUCCESS') {
      await client.query(
        `UPDATE subscriptions 
         SET updated_at = now(),
             retry_count = 0,
             next_execution_at = next_execution_at + INTERVAL '1 day'
         WHERE id = $1`,
        [input.subscriptionId]
      );

      // Re-enqueue next execution (makes it truly recurring)
      await client.query(
        `INSERT INTO payment_execution_queue (subscription_id, subscription_type, due_at)
         SELECT id, subscription_type, next_execution_at
         FROM subscriptions WHERE id = $1`,
        [input.subscriptionId]
      );
    }

    // Update current execution queue entry
    await client.query(
      `UPDATE payment_execution_queue 
       SET status = $1, updated_at = now()
       WHERE subscription_id = $2 AND status = 'PROCESSING'`,
      [input.result === 'SUCCESS' ? 'DONE' : 'FAILED', input.subscriptionId]
    );

    // Write notification to outbox
    const eventType = input.result === 'SUCCESS' ? 'PAYMENT_SUCCEEDED' : 'PAYMENT_FAILED';
    await client.query(
      `INSERT INTO notification_outbox (subscription_id, event_type, delivery_class, payload, idempotency_key)
       VALUES ($1, $2, 'IMMEDIATE', $3, $4)`,
      [
        input.subscriptionId,
        eventType,
        JSON.stringify({ subscriptionId: input.subscriptionId, result: input.result, date: input.executionDate, attemptCount: input.attemptCount }),
        `${input.subscriptionId}-${input.executionDate}-${eventType}`,
      ]
    );

    await client.query('COMMIT');
    console.log(`[recordResult] ${input.subscriptionId} → ${eventType} → written to outbox ✅`);
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

/**
 * Activity 4: Handle final failure — suspend subscription if max retries exceeded,
 * otherwise re-enqueue for next day retry
 */
export async function scheduleRetry(subscriptionId: string): Promise<void> {
  const res = await pool.query(
    `SELECT retry_count, max_retries FROM subscriptions WHERE id = $1`,
    [subscriptionId]
  );
  const sub = res.rows[0];

  if (sub.retry_count + 1 >= sub.max_retries) {
    // Max retries exhausted — suspend subscription
    await pool.query(
      `UPDATE subscriptions SET status = 'SUSPENDED', retry_count = retry_count + 1, updated_at = now() WHERE id = $1`,
      [subscriptionId]
    );
    console.log(`[scheduleRetry] ${subscriptionId} → SUSPENDED (max retries exhausted)`);
  } else {
    // Re-enqueue for next day retry
    await pool.query(
      `UPDATE subscriptions SET retry_count = retry_count + 1, updated_at = now() WHERE id = $1`,
      [subscriptionId]
    );
    // Note: In this PoC, the child workflow handles internal retries with sleep().
    // This activity handles the *outer* retry cycle after all child attempts fail.
    console.log(`[scheduleRetry] ${subscriptionId} → retry_count incremented (${sub.retry_count + 1}/${sub.max_retries})`);
  }
}

/**
 * Activity: Notify a failed payment attempt via HTTP to the platform API.
 * In production, child workflows live in separate services and call the
 * platform API instead of writing directly to its database.
 */
export async function notifyAttemptFailed(input: {
  subscriptionId: string;
  attempt: number;
  maxAttempts: number;
  nextRetryIn: string;
}): Promise<void> {
  const baseUrl = process.env.PLATFORM_API_URL || 'http://localhost:3000';

  const res = await fetch(`${baseUrl}/internal/attempt-failed`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(input),
  });

  if (!res.ok) {
    throw new Error(`notifyAttemptFailed HTTP error: ${res.status} ${await res.text()}`);
  }

  console.log(`[notifyAttemptFailed] ${input.subscriptionId} → attempt ${input.attempt} → notified via API ✅`);
}

/**
 * Bill domain activities (for child workflow)
 * In production, these live in ms-bill-payments (a separate service).
 * They do NOT access the platform's database — they either call external
 * biller APIs or call back to the platform API for state changes.
 */
export async function validateBiller(destinationId: string): Promise<boolean> {
  // In production: call biller registry API
  console.log(`[validateBiller] Checking biller: ${destinationId}`);
  return true;
}

export async function executeCharge(amount: number, destinationId: string): Promise<string> {
  // In production: call biller's charge API
  const success = Math.random() > 0.2; // 80% success rate for PoC
  console.log(`[executeCharge] Amount: ${amount} → Biller: ${destinationId} → ${success ? 'SUCCESS' : 'FAILED'}`);
  return success ? 'SUCCESS' : 'FAILED';
}

// ============================================================
// P2P domain activities (for p2pPaymentWorkflow)
// In production, these live in ms-p2p-payments (a separate service).
// ============================================================

export async function validateP2PRecipient(destinationId: string): Promise<boolean> {
  // In production: call wallet/account service to verify recipient is active
  console.log(`[validateP2PRecipient] Checking wallet: ${destinationId}`);
  // Simulate 95% of recipients are valid
  const valid = Math.random() > 0.05;
  console.log(`[validateP2PRecipient] ${destinationId} → ${valid ? 'ACTIVE' : 'NOT_FOUND'}`);
  return valid;
}

export async function executeP2PTransfer(amount: number, destinationId: string): Promise<string> {
  // In production: call core banking API for wallet-to-wallet transfer
  // Can fail due to: insufficient funds, recipient limit exceeded, system error
  const roll = Math.random();
  let result: string;
  if (roll > 0.15) {
    result = 'SUCCESS';
  } else if (roll > 0.05) {
    result = 'INSUFFICIENT_FUNDS';
  } else {
    result = 'SYSTEM_ERROR';
  }
  console.log(`[executeP2PTransfer] S/${amount} → ${destinationId} → ${result}`);
  return result === 'SUCCESS' ? 'SUCCESS' : 'FAILED';
}
