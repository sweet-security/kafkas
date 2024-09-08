/* eslint-disable @typescript-eslint/no-unused-vars */
import { ClientMetrics, KafkaConsumer, Message, Metadata, TopicPartitionOffset, WatermarkOffsets } from "node-rdkafka";
import { IConsumer } from "../lib/iConsumer";
import { ConsumerRunConfig } from "../types";

export class ConsumerMock implements IConsumer {
    constructor(config: any, timeoutMs?: number) {
        return;
    }

    connect(topics): Promise<Metadata> {
        return Promise.resolve(null);
    }

    disconnect(): Promise<ClientMetrics> {
        return Promise.resolve(null);
    }

    subscribe(topics: string[]) {
        return null;
    }

    commit(): Promise<TopicPartitionOffset[]> {
        return Promise.resolve(null);
    }

    commitOffset(topicPartition: TopicPartitionOffset | TopicPartitionOffset[] | null): Promise<TopicPartitionOffset[]> {
        return Promise.resolve([]);
    }

    commitMessage(msg: TopicPartitionOffset): Promise<TopicPartitionOffset[]> {
        return Promise.resolve([]);
    }

    listen(numberOfMessages: number, autoCommit: boolean): Promise<Message[]> {
        return Promise.resolve([]);
    }

    getOffsets(topic: string, partition: number): Promise<WatermarkOffsets> {
        return Promise.resolve({ highOffset: 100, lowOffset: 0 });
    }

    getConsumer(): KafkaConsumer {
        return null;
    }

    run(config: ConsumerRunConfig): void {
        return;
    }
}
