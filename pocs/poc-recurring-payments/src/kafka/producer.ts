import { Kafka, Producer } from 'kafkajs';

const BROKER = process.env.KAFKA_BROKER || 'localhost:9092';
const TOPIC = process.env.KAFKA_TOPIC || 'notifications';

const kafka = new Kafka({
  clientId: 'poc-recurring-payments',
  brokers: [BROKER],
});

let producer: Producer;

export async function connectProducer(): Promise<void> {
  producer = kafka.producer();
  await producer.connect();
  console.log(`🔌 Kafka producer connected → broker: ${BROKER}, topic: ${TOPIC}`);
}

export async function publishEvent(key: string, value: object): Promise<void> {
  await producer.send({
    topic: TOPIC,
    messages: [
      {
        key,
        value: JSON.stringify(value),
      },
    ],
  });
}

export async function disconnectProducer(): Promise<void> {
  if (producer) {
    await producer.disconnect();
  }
}
