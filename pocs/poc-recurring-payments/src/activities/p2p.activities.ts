import { connectProducer, publishEvent } from '../kafka/producer';

/**
 * Initialize Kafka producer at worker startup.
 * Call this once from the worker before starting.
 */
export async function initP2PKafka(): Promise<void> {
  await connectProducer();
}

/**
 * Validate P2P recipient wallet exists and is active.
 * In production: calls wallet/account service.
 */
export async function validateP2PRecipient(destinationId: string): Promise<boolean> {
  console.log(`[validateP2PRecipient] Checking wallet: ${destinationId}`);
  const valid = Math.random() > 0.05; // 95% valid
  console.log(`[validateP2PRecipient] ${destinationId} → ${valid ? 'ACTIVE' : 'NOT_FOUND'}`);
  return valid;
}

/**
 * Execute P2P wallet-to-wallet transfer.
 * In production: calls core banking API.
 */
export async function executeP2PTransfer(amount: number, destinationId: string): Promise<string> {
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

/**
 * Publish a P2P payment event to Kafka.
 * The P2P domain owns its notifications end-to-end.
 */
export async function publishP2PEvent(input: {
  subscriptionId: string;
  eventType: 'ATTEMPT_FAILED' | 'PAYMENT_SUCCEEDED' | 'PAYMENT_FAILED';
  payload: Record<string, unknown>;
  idempotencyKey?: string;
}): Promise<void> {
  const idempotencyKey = input.idempotencyKey || `${input.subscriptionId}-${input.eventType}-${Date.now()}`;

  const message = {
    domain: 'p2p',
    subscriptionId: input.subscriptionId,
    eventType: input.eventType,
    idempotencyKey,
    payload: input.payload,
    publishedAt: new Date().toISOString(),
  };

  await publishEvent(input.subscriptionId, message);
  console.log(`[P2P→Kafka] 📤 ${input.eventType} | sub: ${input.subscriptionId} | key: ${idempotencyKey}`);
}
