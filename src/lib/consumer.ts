import {
    ClientMetrics,
    KafkaConsumer,
    Message as RdMessage,
    Metadata,
    SubscribeTopicList,
    TopicPartitionOffset,
    WatermarkOffsets,
} from "node-rdkafka";
import { IConsumer } from "./iConsumer";
import { ConsumerRunConfig, IHeaders, Message } from "../types";

export class Consumer implements IConsumer {
    private readonly consumer: KafkaConsumer;
    private readonly consumeTimeout: number;

    /**
     * @param config Node-rdkafka configuration object. Minimum: `{ "metadata.broker.list": "0.0.0.0:9094", "group.id": "test.group" }`
     * @param timeoutMs Consume timeout in ms
     */
    constructor(config: any, timeoutMs?: number) {
        this.consumeTimeout = timeoutMs ? timeoutMs : 1000;

        const consumerConfig = {
            "enable.auto.commit": false,
            "socket.keepalive.enable": true,
            ...config,
        };

        this.consumer = new KafkaConsumer(consumerConfig, {
            "auto.offset.reset": "earliest",
        });
    }

    connect(topics: SubscribeTopicList): Promise<Metadata> {
        // Bus is ready and message(s) can be consumed
        this.consumer.on("ready", () => {
            // Get consume timeout from config (or 5 sec)
            this.consumer.setDefaultConsumeTimeout(this.consumeTimeout);
            this.consumer.subscribe(topics);
        });
        // Connect consumer to kafka
        return new Promise((resolve, reject) => {
            this.consumer.connect(null, (err, metadata) => {
                if (err) {
                    return reject(err);
                } else {
                    return resolve(metadata);
                }
            });
        });
    }

    disconnect(): Promise<ClientMetrics> {
        return new Promise((resolve, reject) => {
            this.consumer.disconnect((err, metrics) => {
                if (err) {
                    // Should not happen
                    reject(err);
                } else {
                    resolve(metrics);
                }
            });
        });
    }

    subscribe(topics: string[]) {
        this.consumer.unsubscribe();
        this.consumer.subscribe(topics);
    }

    commit(): Promise<TopicPartitionOffset[]> {
        return this.commitOffset(null);
    }

    commitOffset(topicPartition: TopicPartitionOffset | TopicPartitionOffset[] | null): Promise<TopicPartitionOffset[]> {
        return new Promise((resolve, reject) => {
            if (topicPartition) {
                this.consumer.commit(topicPartition);
            } else {
                this.consumer.commit();
            }
            this.consumer.committed(undefined, 5000, (err, topicPartitions) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(topicPartitions);
                }
            });
        });
    }

    commitMessage(msg: TopicPartitionOffset): Promise<TopicPartitionOffset[]> {
        return new Promise((resolve, reject) => {
            this.consumer.commitMessage(msg);
            this.consumer.committed(undefined, 5000, (err, topicPartitions) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(topicPartitions);
                }
            });
        });
    }

    listen(numberOfMessages: number, autoCommit: boolean): Promise<RdMessage[]> {
        return new Promise((resolve, reject) => {
            this.consumer.consume(numberOfMessages, (err, messages) => {
                if (err) {
                    reject(err);
                } else {
                    if (autoCommit === true) {
                        this.commit()
                            .then(() => {
                                resolve(messages);
                            })
                            .catch((error) => {
                                reject(error);
                            });
                    } else {
                        resolve(messages);
                    }
                }
            });
        });
    }

    getOffsets(topic: string, partition: number): Promise<WatermarkOffsets> {
        return new Promise((resolve, reject) => {
            this.consumer.queryWatermarkOffsets(topic, partition, 5000, (err, offsets) => {
                if (offsets) {
                    resolve(offsets);
                } else {
                    reject(err);
                }
            });
        });
    }

    getConsumer(): KafkaConsumer {
        return this.consumer;
    }

    //     export interface Message {
    //     key?: Buffer | string | null
    //     value: Buffer | string | null
    //     partition?: number
    //     headers?: IHeaders
    //     timestamp?: string
    // }

    parseMessage(message: RdMessage): Message {
        const parsedHeaders: IHeaders = {};
        message.headers.forEach((header) => {
            for (const key in header) {
                const value = header[key];

                if (parsedHeaders[key] === undefined) {
                    // If key does not exist, add it directly
                    parsedHeaders[key] = value;
                } else {
                    // If key exists, ensure the value is an array and then append
                    if (!Array.isArray(parsedHeaders[key])) {
                        parsedHeaders[key] = [parsedHeaders[key] as string | Buffer];
                    }
                    (parsedHeaders[key] as (string | Buffer)[]).push(value);
                }
            }
        });

        return {
            key: message.key,
            partition: message.partition,
            headers: parsedHeaders,
            timestamp: message.timestamp,
            value: message.value,
        };
    }

    parseMessages(messages: RdMessage[]): Message[] {
        return messages.map(this.parseMessage);
    }

    run(config: ConsumerRunConfig) {
        this.listen(config.messageBatchSize, config.autoCommit)
            .then(async (messages: RdMessage[]) => {
                const parseMessages = this.parseMessages(messages);
                if (config.eachMessage) {
                    for (const message of parseMessages) {
                        await config.eachMessage(message);
                    }
                } else if (config.eachBatch) {
                    await config.eachBatch(parseMessages);
                }
            })
            .finally(() => {
                this.run(config);
            });
    }
}
