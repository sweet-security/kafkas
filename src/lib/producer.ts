import { ClientMetrics, HighLevelProducer, Message as RdMessage, Metadata } from "node-rdkafka";
import { IProducer } from "./iProducer";
import { Message, MessageHeader, ProducerRecord } from "../types";

export class Producer implements IProducer {
    private connected: boolean;
    private readonly prefix: string;
    private readonly producer: HighLevelProducer;

    /**
     * @param config Node-rdkafka configuration object. Minimum: `{ "metadata.broker.list": "0.0.0.0:9094" }`
     * @param topicPrefix Prefix to add before each topic name
     */
    constructor(config: any, topicPrefix?: string) {
        this.connected = false;
        this.prefix = topicPrefix ? topicPrefix : "";

        const producerConfig = {
            "socket.keepalive.enable": true,
            ...config,
        };

        this.producer = new HighLevelProducer(producerConfig, {});
    }

    connect(): Promise<Metadata> {
        return new Promise((resolve, reject) => {
            if (this.producer && this.connected === true) {
                // Do nothing if we are already connected
                resolve(null);
            } else {
                this.producer.setValueSerializer((v) => v);
                this.producer.setKeySerializer((v) => v);

                this.producer.connect(null, (err, metadata) => {
                    if (err) {
                        reject(err);
                    } else {
                        this.connected = true;
                        resolve(metadata);
                    }
                });
            }
        });
    }

    disconnect(): Promise<ClientMetrics> {
        return new Promise((resolve, reject) => {
            this.producer.disconnect((err, data) => {
                this.connected = false;

                if (err) {
                    // Should not happen
                    reject(err);
                } else {
                    resolve(data);
                }
            });
        });
    }

    serializeMessage(message: Message): Partial<RdMessage> {
        const messageHeadersArray: MessageHeader[] = [];

        for (const key in message.headers) {
            const value = message.headers[key];

            if (value === undefined) {
                continue; // Skip undefined values
            }

            if (Array.isArray(value)) {
                // If the value is an array, create a MessageHeader for each item
                value.forEach((v) => {
                    messageHeadersArray.push({ [key]: v });
                });
            } else {
                // If the value is a single string or Buffer, create a single MessageHeader
                messageHeadersArray.push({ [key]: value });
            }
        }

        return {
            key: message.key,
            partition: message.partition,
            headers: messageHeadersArray,
            timestamp: message.timestamp,
            value: (typeof message.value === "string")? Buffer.from(message.value) : message.value
        };
    }

    async send(record: ProducerRecord): Promise<any> {
        return record.messages.map((messageToSend) => this.sendBufferMessage(record.topic, messageToSend));
    }

    async sendBufferMessage(topic: string, message: Message): Promise<number> {
        return new Promise((resolve, reject) => {
            const serializedMessage = this.serializeMessage(message);
            // Create full topic
            const fullTopic = this.prefix + topic;

            // Send message to kafka
            this.producer.produce(
                fullTopic,
                serializedMessage.partition,
                serializedMessage.value,
                serializedMessage.key,
                Date.now(),
                serializedMessage.headers,
                (err, offset) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(offset);
                    }
                },
            );
        });
    }

    getMetadata(topic: string, timeout = 5000): Promise<Metadata> {
        // Get all topics or only the one in parameter
        const allTopics = !topic;
        return new Promise((resolve, reject) => {
            this.producer.getMetadata({ topic, timeout, allTopics }, (err, metadata) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(metadata);
                }
            });
        });
    }
}
