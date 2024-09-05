/* eslint-disable @typescript-eslint/no-unused-vars */
import { KafkaProducerInterface } from "../lib/kafkaProducerInterface";
import { ClientMetrics, Metadata } from "node-rdkafka";

export class KafkaProducerMock implements KafkaProducerInterface {
    constructor(config: any, topicPrefix?: string) {
        return;
    }

    connect(): Promise<Metadata> {
        return Promise.resolve(null);
    }

    disconnect(): Promise<ClientMetrics> {
        return Promise.resolve(null);
    }

    sendMessage(topic: string, message: any, partition: number, key: any): Promise<number> {
        return Promise.resolve(0);
    }

    sendBufferMessage(topic: string, message: any, partition: number, key: any): Promise<number> {
        return Promise.resolve(0);
    }

    getMetadata(topic?: string, timeout?: number): Promise<Metadata> {
        return Promise.resolve(undefined);
    }
}
