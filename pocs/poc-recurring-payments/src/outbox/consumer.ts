import { pool } from '../db/pool';
import { connectProducer, publishEvent, disconnectProducer } from '../kafka/producer';

const POLL_INTERVAL_MS = 2000;
const BATCH_SIZE = 20;

/**
 * Outbox Consumer — polls notification_outbox and publishes events to Kafka topic "notifications".
 * Uses transactional outbox pattern: claim → publish to Kafka → mark as PUBLISHED.
 */
async function consume() {
  await connectProducer();

  console.log('📬 Outbox Consumer started — polling every 2s');
  console.log('   Publishing to Kafka topic: notifications\n');

  let totalPublished = 0;

  // Graceful shutdown
  const shutdown = async () => {
    console.log('\n📬 Shutting down outbox consumer...');
    await disconnectProducer();
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);

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

          const kafkaMessage = {
            id: event.id,
            subscriptionId: event.subscription_id,
            eventType: event.event_type,
            deliveryClass: event.delivery_class,
            payload,
            idempotencyKey: event.idempotency_key,
            createdAt: event.created_at,
            publishedAt: new Date().toISOString(),
          };

          await publishEvent(event.subscription_id, kafkaMessage);

          console.log(`  📤 [${event.event_type}] → Kafka topic:notifications`);
          console.log(`     key: ${event.subscription_id}`);
          console.log(`     payload: ${JSON.stringify(payload)}`);
          console.log(`     idempotency_key: ${event.idempotency_key}`);
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

consume().catch(console.error);
