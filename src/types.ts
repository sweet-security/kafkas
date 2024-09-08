import { Message } from "node-rdkafka";

export type EachBatchHandler = (payload: Message[]) => Promise<void>;
export type EachMessageHandler = (payload: Message) => Promise<void>;

export type ConsumerRunConfig = {
    autoCommit?: boolean;
    autoCommitInterval?: number | null;
    autoCommitThreshold?: number | null;
    eachBatchAutoResolve?: boolean;
    messageBatchSize?: number;
    eachBatch?: EachBatchHandler;
    eachMessage?: EachMessageHandler;
};

export interface ProducerRecord {
    topic: string;
    message: Message;
    ack?: number;
    timeout?: number;
    compression?: CompressionTypes;
    partition?: number;
    key?: string;
}

export enum CompressionTypes {
    None = 0,
    GZIP = 1,
    Snappy = 2,
    LZ4 = 3,
    ZSTD = 4,
}