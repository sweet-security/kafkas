const { maxBy, minBy } = require('lodash')

const extractTopicPartitions = (topics, cluster) => {
  return topics.flatMap(topic => {
    const partitionsMetadata = cluster.findTopicPartitionMetadata(topic)
    return partitionsMetadata.map(partitionMetadata => ({
      topic,
      partitionId: partitionMetadata.partitionId,
    }))
  })
}

const calculateAvgPartitions = (topicsPartitionsCount, membersCount) => {
  return Math.ceil(topicsPartitionsCount / membersCount);
}

const hasImbalance = (assignment, avgPartitions) => {
  return Object.values(assignment).some(
    topicPartitions => Object.values(topicPartitions).flat().length > avgPartitions
  )
}

const unloadOverloadedMembers = (assignment, avgPartitions) => {
  const removedPartitions = []
  for (const memberId in assignment) {
    const memberAssignedPartitionCount = getMemberAssignedPartitionCount(assignment, memberId)
    const partitionsToRemove = memberAssignedPartitionCount - avgPartitions

    if (partitionsToRemove > 0) {
      let partitionsRemovedCount = 0

      while (partitionsRemovedCount < partitionsToRemove) {
        // Find the topic with the largest partition count in the member assignment
        const [topicWithMostPartitions, partitions] = maxBy(Object.entries(assignment[memberId]), [
          ([_, assignedTopicPartitions]) => assignedTopicPartitions.length,
        ])

        const removedPartitionId = partitions.pop()
        partitionsRemovedCount++

        if (removedPartitionId !== undefined) {
          removedPartitions.push({ partitionId: removedPartitionId, topic: topicWithMostPartitions })
        }

        if (partitions.length === 0) {
          delete assignment[memberId][topicWithMostPartitions]
        }
      }
    }
  }

  return removedPartitions
}

const assignUnassignedPartitions = (assignment, unassignedPartitions, members) => {
  for (const unassignedPartition of unassignedPartitions) {
    const memberWithLeastPartitions = minBy(members, member =>
      getMemberAssignedPartitionCount(assignment, member.memberId)
    )?.memberId

    if (!memberWithLeastPartitions) {
      continue
    }

    if (!assignment[memberWithLeastPartitions][unassignedPartition.topic]) {
      assignment[memberWithLeastPartitions][unassignedPartition.topic] = []
    }
    assignment[memberWithLeastPartitions][unassignedPartition.topic].push(
      unassignedPartition.partitionId
    )
  }
}

const getMemberAssignedPartitionCount = (assignment, memberId) => {
  return Object.values(assignment[memberId] || {}).flat().length
}

const getUnassignedPartitions = (currentAssignment, topicsPartitions) => {
  const assignedTopicPartitions = Object.values(currentAssignment)
  return topicsPartitions.filter(
    topicPartition =>
      !assignedTopicPartitions.some(assignedTopicPartition =>
        assignedTopicPartition[topicPartition.topic]?.includes(topicPartition.partitionId)
      )
  )
}

module.exports = {
  extractTopicPartitions,
  calculateAvgPartitions,
  getUnassignedPartitions,
  unloadOverloadedMembers,
  hasImbalance,
  assignUnassignedPartitions,
}
