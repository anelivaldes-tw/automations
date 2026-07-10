import { Kafka, Producer } from 'kafkajs';

const BROKER = process.env.KAFKA_BROKER || 'localhost:9092';
const TOPIC = process.env.KAFKA_TOPIC || 'notifications';

const kafka = new Kafka({
  clientId: 'poc-recurring-payments',
  brokers: [BROKER],
});

let producer: Producer;
let connected = false;

export async function connectProducer(): Promise<void> {
  if (connected) return;
  producer = kafka.producer({
    idempotent: true, // Enable Kafka-level idempotence (exactly-once per partition)
  });
  await producer.connect();
  connected = true;
  console.log(`🔌 Kafka producer connected → broker: ${BROKER}, topic: ${TOPIC} (idempotent: true)`);
}

export async function publishEvent(key: string, value: object, topic?: string): Promise<void> {
  if (!connected) {
    throw new Error('Kafka producer not connected. Call connectProducer() at worker startup.');
  }
  await producer.send({
    topic: topic || TOPIC,
    messages: [
      {
        key,
        value: JSON.stringify(value),
      },
    ],
  });
}

export async function disconnectProducer(): Promise<void> {
  if (producer && connected) {
    await producer.disconnect();
    connected = false;
  }
}
