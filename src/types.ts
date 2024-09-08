export type MessageHeader = { [key: string]: string | Buffer };

export interface IHeaders {
    [key: string]: Buffer | string | (Buffer | string)[] | undefined;
}

export interface Message {
    key: Buffer | string | null;
    value: Buffer | string | null;
    partition?: number;
    headers?: IHeaders;
    timestamp?: number;
}

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
    messages: Message[];
    ack?: number;
    timeout?: number;
    compression?: CompressionTypes;
    partition?: number;
    key?: string | null;
}

export enum CompressionTypes {
    None = 0,
    GZIP = 1,
    Snappy = 2,
    LZ4 = 3,
    ZSTD = 4,
}
