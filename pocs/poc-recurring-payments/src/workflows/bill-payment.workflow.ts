import { proxyActivities, sleep, isCancellation, defineSignal, defineQuery, setHandler } from '@temporalio/workflow';
import type { PaymentExecutionInput } from './recurring-payment.workflow';

const RETRY_DELAY = '1 minute'; // En producción: '1 day'

// Domain activities (biller interaction)
const activities = proxyActivities<{
  validateBiller: (destinationId: string) => Promise<boolean>;
  executeCharge: (amount: number, destinationId: string) => Promise<string>;
}>({
  startToCloseTimeout: '15s',
  retry: { maximumAttempts: 1 },
});

// Kafka publish activity — separate retry policy (more tolerant to transient failures)
const { publishBillEvent } = proxyActivities<{
  publishBillEvent: (input: { subscriptionId: string; eventType: string; payload: Record<string, unknown>; idempotencyKey?: string }) => Promise<void>;
}>({
  startToCloseTimeout: '10s',
  retry: {
    maximumAttempts: 5,
    initialInterval: '1s',
    backoffCoefficient: 2,
    maximumInterval: '30s',
  },
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
    await publishBillEvent({
      subscriptionId: input.subscriptionId,
      eventType: 'PAYMENT_FAILED',
      idempotencyKey: `${input.subscriptionId}-${input.executionDate}-INVALID_BILLER`,
      payload: { reason: 'INVALID_BILLER', destinationId: input.destinationId },
    });
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
        // Notify success to Kafka
        await publishBillEvent({
          subscriptionId: input.subscriptionId,
          eventType: 'PAYMENT_SUCCEEDED',
          idempotencyKey: `${input.subscriptionId}-${input.executionDate}-SUCCESS-attempt${attempt}`,
          payload: { amount: currentAmount, attempt, destinationId: input.destinationId },
        });
        return { status: 'SUCCESS', attemptCount: attempt };
      }

      // Failed — notify user via Kafka and retry
      if (attempt < maxAttempts) {
        await publishBillEvent({
          subscriptionId: input.subscriptionId,
          eventType: 'ATTEMPT_FAILED',
          idempotencyKey: `${input.subscriptionId}-${input.executionDate}-ATTEMPT_FAILED-${attempt}`,
          payload: {
            attempt,
            maxAttempts,
            nextRetryIn: RETRY_DELAY,
            message: `Tu pago falló (intento ${attempt}/${maxAttempts}). Reintentaremos en ${RETRY_DELAY}.`,
          },
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
  // Notify final failure to Kafka
  await publishBillEvent({
    subscriptionId: input.subscriptionId,
    eventType: 'PAYMENT_FAILED',
    idempotencyKey: `${input.subscriptionId}-${input.executionDate}-PAYMENT_FAILED`,
    payload: { amount: currentAmount, attempts: maxAttempts, destinationId: input.destinationId },
  });
  return { status: 'FAILED', attemptCount: maxAttempts };
}
