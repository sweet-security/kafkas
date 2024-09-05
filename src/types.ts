import { Message } from 'node-rdkafka';

export type EachBatchHandler = (payload: Message[]) => Promise<void>
export type EachMessageHandler = (payload: Message) => Promise<void>

export type ConsumerRunConfig = {
  autoCommit?: boolean
  autoCommitInterval?: number | null
  autoCommitThreshold?: number | null
  eachBatchAutoResolve?: boolean
  messageBatchSize?: number
  eachBatch?: EachBatchHandler
  eachMessage?: EachMessageHandler
}