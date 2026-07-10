import { pool } from '../db/pool';
import { connectProducer, publishEvent } from '../kafka/producer';

/**
 * Initialize Kafka producer for platform worker.
 * Only needed if publishResult=true is used.
 */
export async function initPlatformKafka(): Promise<void> {
  await connectProducer();
}

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
 * Platform only manages subscription state and re-enqueue.
 * Notifications are handled by each domain's child workflow.
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

    await client.query('COMMIT');
    console.log(`[recordResult] ${input.subscriptionId} → ${input.result} ✅`);
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

/**
 * Activity 4: Handle final failure — suspend subscription if max retries exceeded
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
    // Increment retry count
    await pool.query(
      `UPDATE subscriptions SET retry_count = retry_count + 1, updated_at = now() WHERE id = $1`,
      [subscriptionId]
    );
    console.log(`[scheduleRetry] ${subscriptionId} → retry_count incremented (${sub.retry_count + 1}/${sub.max_retries})`);
  }
}

/**
 * Activity 5: Publish platform-level event to Kafka.
 * Only called when publishResult=true in the workflow input.
 * Publishes the final outcome of the workflow execution.
 */
export async function publishPlatformEvent(input: {
  subscriptionId: string;
  eventType: string;
  idempotencyKey?: string;
  payload: Record<string, unknown>;
}): Promise<void> {
  const idempotencyKey = input.idempotencyKey || `${input.subscriptionId}-platform-${input.eventType}-${Date.now()}`;

  const message = {
    domain: 'platform',
    subscriptionId: input.subscriptionId,
    eventType: input.eventType,
    idempotencyKey,
    payload: input.payload,
    publishedAt: new Date().toISOString(),
  };

  await publishEvent(input.subscriptionId, message);
  console.log(`[Platform→Kafka] 📤 ${input.eventType} | sub: ${input.subscriptionId} | key: ${idempotencyKey}`);
}
