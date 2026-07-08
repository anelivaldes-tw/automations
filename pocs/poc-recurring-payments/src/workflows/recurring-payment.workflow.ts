import { proxyActivities, executeChild, defineQuery, setHandler, upsertSearchAttributes } from '@temporalio/workflow';
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
  P2P:  { workflowName: 'p2pPaymentWorkflow', taskQueue: 'payments-p2p' },
  MOCK: { workflowName: 'billPaymentWorkflow', taskQueue: 'payments-bill' },
};

// --- Query: inspect parent workflow state ---
export const getExecutionStatusQuery = defineQuery<{
  subscriptionId: string;
  subscriptionType: string;
  executionDate: string;
  phase: 'VALIDATING' | 'EXECUTING_CHILD' | 'RECORDING_RESULT' | 'COMPLETED';
  result?: string;
}>('getExecutionStatus');

export interface PaymentExecutionInput {
  subscriptionId: string;
  subscriptionType: string;
  executionDate: string;
  destinationId: string;
  amount: number;
  maxRetries: number;
  userId?: string;
  metadata: Record<string, unknown>;
}

export async function recurringPaymentWorkflow(input: PaymentExecutionInput): Promise<string> {
  let phase: 'VALIDATING' | 'EXECUTING_CHILD' | 'RECORDING_RESULT' | 'COMPLETED' = 'VALIDATING';
  let finalResult: string | undefined;

  // Register query handler
  setHandler(getExecutionStatusQuery, () => ({
    subscriptionId: input.subscriptionId,
    subscriptionType: input.subscriptionType,
    executionDate: input.executionDate,
    phase,
    result: finalResult,
  }));

  // Upsert Search Attributes for visibility in Temporal UI / queries
  upsertSearchAttributes({
    // Custom search attributes (must be registered in Temporal server)
    // These allow: `temporal workflow list -q 'userId="user123"'`
    ...(input.userId ? { userId: [input.userId] } : {}),
    subscriptionType: [input.subscriptionType],
  });

  // Activity 1: Validate subscription is still active
  const valid = await validateSubscription(input.subscriptionId);
  if (!valid) {
    finalResult = 'SKIPPED_INACTIVE';
    phase = 'COMPLETED';
    return 'SKIPPED_INACTIVE';
  }

  // Activity 2: Resolve strategy and execute child workflow
  phase = 'EXECUTING_CHILD';
  const strategy = STRATEGY_MAP[input.subscriptionType];
  if (!strategy) {
    throw new Error(`Unknown subscription type: ${input.subscriptionType}`);
  }

  let result: string;
  let attemptCount = 0;
  try {
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
  phase = 'RECORDING_RESULT';
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

  finalResult = result;
  phase = 'COMPLETED';
  return result;
}
