import { proxyActivities, executeChild } from '@temporalio/workflow';
import { ChildWorkflowCancellationType } from '@temporalio/workflow';
import type * as activities from '../activities';

// Proxy activities for the platform worker
const { validateSubscription, recordPaymentResult, scheduleRetry } = proxyActivities<typeof activities>({
  startToCloseTimeout: '30s',
  retry: { maximumAttempts: 3 },
});

// Strategy Resolver — maps subscription_type to child workflow + task queue
const STRATEGY_MAP: Record<string, { workflowName: string; taskQueue: string }> = {
  BILL: { workflowName: 'billPaymentWorkflow', taskQueue: 'payments-bill' },
  MOCK: { workflowName: 'mockPaymentWorkflow', taskQueue: 'payments-bill' }, // same queue for PoC
};

export interface PaymentExecutionInput {
  subscriptionId: string;
  subscriptionType: string;
  executionDate: string;
  destinationId: string;
  amount: number;
  maxRetries: number;
  metadata: Record<string, unknown>;
}

export async function recurringPaymentWorkflow(input: PaymentExecutionInput): Promise<string> {
  // Activity 1: Validate subscription is still active
  const valid = await validateSubscription(input.subscriptionId);
  if (!valid) {
    return 'SKIPPED_INACTIVE';
  }

  // Activity 2: Resolve strategy and execute child workflow
  const strategy = STRATEGY_MAP[input.subscriptionType];
  if (!strategy) {
    throw new Error(`Unknown subscription type: ${input.subscriptionType}`);
  }

  let result: string;
  let attemptCount = 0;
  try {
    // Child returns { status, attemptCount } — the child handles retries internally
    const childResult = await executeChild(strategy.workflowName, {
      args: [input],
      taskQueue: strategy.taskQueue,
      workflowId: `${input.subscriptionId}-${input.executionDate}-${input.subscriptionType}`,
      cancellationType: ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
    });
    result = childResult.status;
    attemptCount = childResult.attemptCount;
  } catch (err) {
    result = 'FAILED';
  }

  // Activity 3: Record the final result (atomic TX: update subscription + write outbox)
  await recordPaymentResult({
    subscriptionId: input.subscriptionId,
    executionDate: input.executionDate,
    result,
    attemptCount,
  });

  // Activity 4: Schedule retry if all attempts exhausted (not for cancelled)
  if (result === 'FAILED') {
    await scheduleRetry(input.subscriptionId);
  }

  return result;
}
