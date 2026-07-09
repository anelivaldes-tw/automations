import { proxyActivities, sleep, isCancellation } from '@temporalio/workflow';
import type { PaymentExecutionInput } from './recurring-payment.workflow';

// Domain activities (wallet/transfer interaction)
const activities = proxyActivities<{
  validateP2PRecipient: (destinationId: string) => Promise<boolean>;
  executeP2PTransfer: (amount: number, destinationId: string) => Promise<string>;
}>({
  startToCloseTimeout: '10s',
  retry: { maximumAttempts: 1 },
});

// Kafka publish activity — separate retry policy (more tolerant to transient failures)
const { publishP2PEvent } = proxyActivities<{
  publishP2PEvent: (input: { subscriptionId: string; eventType: string; payload: Record<string, unknown>; idempotencyKey?: string }) => Promise<void>;
}>({
  startToCloseTimeout: '10s',
  retry: {
    maximumAttempts: 5,
    initialInterval: '1s',
    backoffCoefficient: 2,
    maximumInterval: '30s',
  },
});

const RETRY_DELAY = '1 minute'; // En producción: '1 day'

export interface P2PPaymentResult {
  status: 'SUCCESS' | 'FAILED' | 'CANCELLED';
  attemptCount: number;
}

/**
 * P2P Payment Child Workflow
 * Transfers money between Yape users (e.g., recurring allowance to family member).
 * Owns its notifications end-to-end — publishes directly to Kafka.
 */
export async function p2pPaymentWorkflow(input: PaymentExecutionInput): Promise<P2PPaymentResult> {
  const maxAttempts = input.maxRetries || 3;
  console.log(`[P2P] Starting transfer for subscription ${input.subscriptionId} → ${input.destinationId} (S/${input.amount})`);

  // Step 1: Validate recipient wallet exists and is active
  const valid = await activities.validateP2PRecipient(input.destinationId);
  if (!valid) {
    console.log(`[P2P] ❌ Recipient ${input.destinationId} not found or inactive`);
    await publishP2PEvent({
      subscriptionId: input.subscriptionId,
      eventType: 'PAYMENT_FAILED',
      idempotencyKey: `${input.subscriptionId}-${input.executionDate}-INVALID_RECIPIENT`,
      payload: { reason: 'INVALID_RECIPIENT', destinationId: input.destinationId },
    });
    return { status: 'FAILED', attemptCount: 0 };
  }

  // Step 2: Execute transfer with retries
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    console.log(`[P2P] Attempt ${attempt}/${maxAttempts} — transferring S/${input.amount} to ${input.destinationId}`);

    try {
      const result = await activities.executeP2PTransfer(input.amount, input.destinationId);

      if (result === 'SUCCESS') {
        console.log(`[P2P] ✅ Transfer successful on attempt ${attempt}`);
        await publishP2PEvent({
          subscriptionId: input.subscriptionId,
          eventType: 'PAYMENT_SUCCEEDED',
          idempotencyKey: `${input.subscriptionId}-${input.executionDate}-SUCCESS-attempt${attempt}`,
          payload: { amount: input.amount, attempt, destinationId: input.destinationId },
        });
        return { status: 'SUCCESS', attemptCount: attempt };
      }

      // Failed — notify and retry
      if (attempt < maxAttempts) {
        await publishP2PEvent({
          subscriptionId: input.subscriptionId,
          eventType: 'ATTEMPT_FAILED',
          idempotencyKey: `${input.subscriptionId}-${input.executionDate}-ATTEMPT_FAILED-${attempt}`,
          payload: {
            attempt,
            maxAttempts,
            nextRetryIn: RETRY_DELAY,
            message: `Tu transferencia falló (intento ${attempt}/${maxAttempts}). Reintentaremos en ${RETRY_DELAY}.`,
          },
        });
        console.log(`[P2P] ⏳ Transfer failed, retrying in ${RETRY_DELAY}...`);
        await sleep(RETRY_DELAY);
      }
    } catch (err) {
      if (isCancellation(err)) {
        console.log(`[P2P] 🚫 Cancelled during attempt ${attempt}`);
        return { status: 'CANCELLED', attemptCount: attempt };
      }
      throw err;
    }
  }

  console.log(`[P2P] ❌ All ${maxAttempts} attempts exhausted`);
  // Notify final failure
  await publishP2PEvent({
    subscriptionId: input.subscriptionId,
    eventType: 'PAYMENT_FAILED',
    idempotencyKey: `${input.subscriptionId}-${input.executionDate}-PAYMENT_FAILED`,
    payload: { amount: input.amount, attempts: maxAttempts, destinationId: input.destinationId },
  });
  return { status: 'FAILED', attemptCount: maxAttempts };
}
