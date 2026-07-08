import { pool } from '../db/pool';

const POLL_INTERVAL_MS = 2000;
const BATCH_SIZE = 20;

/**
 * Outbox Consumer — polls notification_outbox and "publishes" events.
 * In production: this would be a CDC/Debezium connector or a Kafka producer.
 * Here it simulates publishing by logging and marking as PUBLISHED.
 */
async function consume() {
  console.log('📬 Outbox Consumer started — polling every 2s');
  console.log('   In production: this publishes to Kafka/SQS for downstream consumers');
  console.log('   (email service, push notifications, analytics, audit trail)\n');

  let totalPublished = 0;

  while (true) {
    try {
      // Claim pending events (SKIP LOCKED for multi-instance in prod)
      const res = await pool.query(
        `UPDATE notification_outbox
         SET status = 'PUBLISHED', published_at = now()
         WHERE id IN (
           SELECT id FROM notification_outbox
           WHERE status = 'PENDING'
           ORDER BY created_at
           LIMIT $1
         )
         RETURNING id, subscription_id, event_type, delivery_class, payload, idempotency_key, created_at`,
        [BATCH_SIZE]
      );

      if (res.rows.length > 0) {
        for (const event of res.rows) {
          totalPublished++;
          const payload = typeof event.payload === 'string' ? JSON.parse(event.payload) : event.payload;
          
          // Simulate routing to different consumers based on event_type
          const consumer = getConsumer(event.event_type);
          
          console.log(`  📤 [${event.event_type}] → ${consumer}`);
          console.log(`     subscription: ${event.subscription_id}`);
          console.log(`     payload: ${JSON.stringify(payload)}`);
          console.log(`     idempotency_key: ${event.idempotency_key}`);
          console.log(`     delivery: ${event.delivery_class} | published_at: ${new Date().toISOString()}`);
          console.log('');
        }

        console.log(`  ✅ Batch published: ${res.rows.length} events (total: ${totalPublished})\n`);
      }
    } catch (err) {
      console.error('Outbox consumer error:', err);
    }

    await new Promise(resolve => setTimeout(resolve, POLL_INTERVAL_MS));
  }
}

/**
 * Simulates routing events to different downstream consumers
 */
function getConsumer(eventType: string): string {
  const routes: Record<string, string> = {
    'PAYMENT_SUCCEEDED': 'kafka://payments.events → [ms-notifications (push), ms-analytics (metrics)]',
    'PAYMENT_FAILED': 'kafka://payments.events → [ms-notifications (push + email), ms-support (alert)]',
    'ATTEMPT_FAILED': 'kafka://payments.retries → [ms-notifications (push: "reintentaremos mañana")]',
    'SUBSCRIPTION_SUSPENDED': 'kafka://subscriptions.lifecycle → [ms-notifications (email), ms-crm (churn risk)]',
  };
  return routes[eventType] || `kafka://payments.unknown → [unrouted]`;
}

consume().catch(console.error);
