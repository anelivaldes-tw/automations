import { proxyActivities, sleep, isCancellation } from '@temporalio/workflow';
import type { PaymentExecutionInput } from './recurring-payment.workflow';

const RETRY_DELAY = '1 minute'; // En producción: '1 day'

const activities = proxyActivities<{
  validateBiller: (destinationId: string) => Promise<boolean>;
  executeCharge: (amount: number, destinationId: string) => Promise<string>;
  notifyAttemptFailed: (input: { subscriptionId: string; attempt: number; maxAttempts: number; nextRetryIn: string }) => Promise<void>;
}>({
  startToCloseTimeout: '15s',
  retry: { maximumAttempts: 1 },
});

export interface BillPaymentResult {
  status: 'SUCCESS' | 'FAILED' | 'CANCELLED';
  attemptCount: number;
}

export async function billPaymentWorkflow(input: PaymentExecutionInput): Promise<BillPaymentResult> {
  const maxAttempts = input.maxRetries || 3;
  console.log(`[BillPayment] Starting for subscription ${input.subscriptionId} (max attempts: ${maxAttempts})`);

  // Step 1: Validate biller exists
  const valid = await activities.validateBiller(input.destinationId);
  if (!valid) {
    return { status: 'FAILED', attemptCount: 0 };
  }

  // Step 2: Execute charge with retries (cancellation-aware)
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    console.log(`[BillPayment] Attempt ${attempt}/${maxAttempts} for ${input.subscriptionId}`);

    try {
      const chargeResult = await activities.executeCharge(input.amount, input.destinationId);

      if (chargeResult === 'SUCCESS') {
        console.log(`[BillPayment] ✅ Success on attempt ${attempt}`);
        return { status: 'SUCCESS', attemptCount: attempt };
      }

      // Failed — notify user via outbox
      if (attempt < maxAttempts) {
        await activities.notifyAttemptFailed({
          subscriptionId: input.subscriptionId,
          attempt,
          maxAttempts,
          nextRetryIn: RETRY_DELAY,
        });

        console.log(`[BillPayment] ⏳ Failed attempt ${attempt}, retrying in ${RETRY_DELAY}...`);
        await sleep(RETRY_DELAY); // Durable timer — cancellation will interrupt this
      }
    } catch (err) {
      if (isCancellation(err)) {
        console.log(`[BillPayment] 🚫 Cancelled during attempt ${attempt}`);
        return { status: 'CANCELLED', attemptCount: attempt };
      }
      throw err;
    }
  }

  console.log(`[BillPayment] ❌ All ${maxAttempts} attempts exhausted`);
  return { status: 'FAILED', attemptCount: maxAttempts };
}
