import { proxyActivities, sleep, isCancellation, defineSignal, defineQuery, setHandler } from '@temporalio/workflow';
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

// --- Signals: allow external systems to modify workflow behavior mid-flight ---
export const updateAmountSignal = defineSignal<[number]>('updateAmount');

// --- Queries: allow external systems to inspect workflow state without waiting ---
export const getProgressQuery = defineQuery<{
  currentAttempt: number;
  maxAttempts: number;
  currentAmount: number;
  status: 'RUNNING' | 'WAITING_RETRY' | 'COMPLETED';
  lastAttemptResult?: string;
}>('getProgress');

export interface BillPaymentResult {
  status: 'SUCCESS' | 'FAILED' | 'CANCELLED';
  attemptCount: number;
}

export async function billPaymentWorkflow(input: PaymentExecutionInput): Promise<BillPaymentResult> {
  const maxAttempts = input.maxRetries || 3;

  // Mutable state (Signals can modify these)
  let currentAmount = input.amount;
  let currentAttempt = 0;
  let status: 'RUNNING' | 'WAITING_RETRY' | 'COMPLETED' = 'RUNNING';
  let lastAttemptResult: string | undefined;

  // Register Signal handler: allows changing amount mid-flight (e.g., partial payment)
  setHandler(updateAmountSignal, (newAmount: number) => {
    console.log(`[BillPayment] 📡 Signal received: amount updated ${currentAmount} → ${newAmount}`);
    currentAmount = newAmount;
  });

  // Register Query handler: allows checking progress without blocking
  setHandler(getProgressQuery, () => ({
    currentAttempt,
    maxAttempts,
    currentAmount,
    status,
    lastAttemptResult,
  }));

  console.log(`[BillPayment] Starting for subscription ${input.subscriptionId} (max attempts: ${maxAttempts})`);

  // Step 1: Validate biller exists
  const valid = await activities.validateBiller(input.destinationId);
  if (!valid) {
    status = 'COMPLETED';
    return { status: 'FAILED', attemptCount: 0 };
  }

  // Step 2: Execute charge with retries (cancellation-aware)
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    currentAttempt = attempt;
    status = 'RUNNING';
    console.log(`[BillPayment] Attempt ${attempt}/${maxAttempts} for ${input.subscriptionId} (amount: S/${currentAmount})`);

    try {
      const chargeResult = await activities.executeCharge(currentAmount, input.destinationId);
      lastAttemptResult = chargeResult;

      if (chargeResult === 'SUCCESS') {
        console.log(`[BillPayment] ✅ Success on attempt ${attempt}`);
        status = 'COMPLETED';
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

        status = 'WAITING_RETRY';
        console.log(`[BillPayment] ⏳ Failed attempt ${attempt}, retrying in ${RETRY_DELAY}...`);
        await sleep(RETRY_DELAY); // Durable timer — cancellation will interrupt this
      }
    } catch (err) {
      if (isCancellation(err)) {
        console.log(`[BillPayment] 🚫 Cancelled during attempt ${attempt}`);
        status = 'COMPLETED';
        return { status: 'CANCELLED', attemptCount: attempt };
      }
      throw err;
    }
  }

  console.log(`[BillPayment] ❌ All ${maxAttempts} attempts exhausted`);
  status = 'COMPLETED';
  return { status: 'FAILED', attemptCount: maxAttempts };
}
