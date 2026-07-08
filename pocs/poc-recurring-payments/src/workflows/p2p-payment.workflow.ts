import { proxyActivities, sleep, isCancellation } from '@temporalio/workflow';
import type { PaymentExecutionInput } from './recurring-payment.workflow';

const activities = proxyActivities<{
  validateP2PRecipient: (destinationId: string) => Promise<boolean>;
  executeP2PTransfer: (amount: number, destinationId: string) => Promise<string>;
  notifyAttemptFailed: (input: { subscriptionId: string; attempt: number; maxAttempts: number; nextRetryIn: string }) => Promise<void>;
}>({
  startToCloseTimeout: '10s',
  retry: { maximumAttempts: 1 },
});

const RETRY_DELAY = '1 minute'; // En producción: '1 day'

export interface P2PPaymentResult {
  status: 'SUCCESS' | 'FAILED' | 'CANCELLED';
  attemptCount: number;
}

/**
 * P2P Payment Child Workflow
 * Transfers money between Yape users (e.g., recurring allowance to family member).
 * Simpler than BILL: validates recipient wallet, executes transfer.
 * Same retry/cancellation patterns as BILL to demonstrate strategy reuse.
 */
export async function p2pPaymentWorkflow(input: PaymentExecutionInput): Promise<P2PPaymentResult> {
  const maxAttempts = input.maxRetries || 3;
  console.log(`[P2P] Starting transfer for subscription ${input.subscriptionId} → ${input.destinationId} (S/${input.amount})`);

  // Step 1: Validate recipient wallet exists and is active
  const valid = await activities.validateP2PRecipient(input.destinationId);
  if (!valid) {
    console.log(`[P2P] ❌ Recipient ${input.destinationId} not found or inactive`);
    return { status: 'FAILED', attemptCount: 0 };
  }

  // Step 2: Execute transfer with retries
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    console.log(`[P2P] Attempt ${attempt}/${maxAttempts} — transferring S/${input.amount} to ${input.destinationId}`);

    try {
      const result = await activities.executeP2PTransfer(input.amount, input.destinationId);

      if (result === 'SUCCESS') {
        console.log(`[P2P] ✅ Transfer successful on attempt ${attempt}`);
        return { status: 'SUCCESS', attemptCount: attempt };
      }

      // Failed — notify and retry
      if (attempt < maxAttempts) {
        await activities.notifyAttemptFailed({
          subscriptionId: input.subscriptionId,
          attempt,
          maxAttempts,
          nextRetryIn: RETRY_DELAY,
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
  return { status: 'FAILED', attemptCount: maxAttempts };
}
