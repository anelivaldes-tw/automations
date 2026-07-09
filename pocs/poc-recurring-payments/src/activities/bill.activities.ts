import { connectProducer, publishEvent, disconnectProducer } from '../kafka/producer';

// Initialize Kafka connection for this domain
let kafkaReady = false;

async function ensureKafka() {
  if (!kafkaReady) {
    await connectProducer();
    kafkaReady = true;
  }
}

/**
 * Validate biller exists and is reachable.
 * In production: calls biller registry API.
 */
export async function validateBiller(destinationId: string): Promise<boolean> {
  console.log(`[validateBiller] Checking biller: ${destinationId}`);
  return true;
}

/**
 * Execute charge against biller.
 * In production: calls biller's charge API.
 */
export async function executeCharge(amount: number, destinationId: string): Promise<string> {
  const success = Math.random() > 0.2; // 80% success rate for PoC
  console.log(`[executeCharge] Amount: ${amount} → Biller: ${destinationId} → ${success ? 'SUCCESS' : 'FAILED'}`);
  return success ? 'SUCCESS' : 'FAILED';
}

/**
 * Publish a bill payment event to Kafka.
 * The bill domain owns its notifications end-to-end.
 */
export async function publishBillEvent(input: {
  subscriptionId: string;
  eventType: 'ATTEMPT_FAILED' | 'PAYMENT_SUCCEEDED' | 'PAYMENT_FAILED';
  payload: Record<string, unknown>;
}): Promise<void> {
  await ensureKafka();

  const message = {
    domain: 'bill',
    subscriptionId: input.subscriptionId,
    eventType: input.eventType,
    payload: input.payload,
    publishedAt: new Date().toISOString(),
  };

  await publishEvent(input.subscriptionId, message);
  console.log(`[Bill→Kafka] 📤 ${input.eventType} | sub: ${input.subscriptionId}`);
}
