import { ClientMetrics, HighLevelProducer, Metadata } from "node-rdkafka";
import { IProducer } from "./kafkaProducerInterface";
import { ProducerRecord } from "../types";

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

    send(record: ProducerRecord): Promise<number> {
        const message = Buffer.from(JSON.stringify(record.message));
        return this.sendBufferMessage(record.topic, message, record.partition, record.key);
    }

    sendBufferMessage(topic: string, message: any, partition: number, key: any): Promise<number> {
        return new Promise((resolve, reject) => {
            // Create full topic
            const fullTopic = this.prefix + topic;

            // Send message to kafka
            this.producer.produce(fullTopic, partition, message, key, Date.now(), (err, offset) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(offset);
                }
            });
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
