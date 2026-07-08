/**
 * Quick script to create test subscriptions via the API
 * Usage: npx ts-node src/scripts/create-subscription.ts
 */

async function createTestSubscriptions() {
  const baseUrl = 'http://localhost:3000';

  const subscriptions = [
    { userId: 'user-001', subscriptionType: 'BILL', destinationId: 'biller-electricity', amount: 120.50 },
    { userId: 'user-001', subscriptionType: 'BILL', destinationId: 'biller-water', amount: 45.00 },
    { userId: 'user-002', subscriptionType: 'BILL', destinationId: 'biller-internet', amount: 89.90 },
  ];

  for (const sub of subscriptions) {
    const res = await fetch(`${baseUrl}/subscriptions`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(sub),
    });
    const data = await res.json() as { id: string; nextExecution: string };
    console.log(`✅ Created: ${data.id} → ${sub.subscriptionType} → $${sub.amount} → next: ${data.nextExecution}`);
  }

  console.log('\n📋 Done! Subscriptions will execute in ~10 seconds.');
  console.log('   Watch the scheduler and workers for output.');
}

createTestSubscriptions().catch(console.error);
