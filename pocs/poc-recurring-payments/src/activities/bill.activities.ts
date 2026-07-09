import { connectProducer, publishEvent } from '../kafka/producer';

/**
 * Initialize Kafka producer at worker startup.
 * Call this once from the worker before starting.
 */
export async function initBillKafka(): Promise<void> {
  await connectProducer();
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
  idempotencyKey?: string;
}): Promise<void> {
  const idempotencyKey = input.idempotencyKey || `${input.subscriptionId}-${input.eventType}-${Date.now()}`;

  const message = {
    domain: 'bill',
    subscriptionId: input.subscriptionId,
    eventType: input.eventType,
    idempotencyKey,
    payload: input.payload,
    publishedAt: new Date().toISOString(),
  };

  await publishEvent(input.subscriptionId, message);
  console.log(`[Bill→Kafka] 📤 ${input.eventType} | sub: ${input.subscriptionId} | key: ${idempotencyKey}`);
}
