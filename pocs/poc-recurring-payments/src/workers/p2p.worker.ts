import { Worker } from '@temporalio/worker';
import * as p2pActivities from '../activities/p2p.activities';
import * as path from 'path';

async function run() {
  // Connect Kafka producer before starting the worker
  await p2pActivities.initP2PKafka();

  const worker = await Worker.create({
    workflowsPath: path.resolve(__dirname, '../workflows'),
    activities: p2pActivities,
    taskQueue: 'payments-p2p',
  });

  console.log('🚀 P2P Worker started (task queue: payments-p2p)');
  console.log('   Activities: validateP2PRecipient, executeP2PTransfer, publishP2PEvent');
  await worker.run();
}

run().catch((err) => {
  console.error('❌ P2P worker failed:', err);
  process.exit(1);
});
