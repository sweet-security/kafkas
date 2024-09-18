const { orderBy } = require('lodash')
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
      
      // Sort by partition count
      const sortedAssignedTopics = orderBy(
        Object.entries(assignment[memberId]),
        [([_, assignedTopicPartitions]) => assignedTopicPartitions.length],
        'desc'
      )

      for (const topic in sortedAssignedTopics) {
        const removedPartitionId = sortedAssignedTopics[topic].pop()
        partitionsRemovedCount++

        if (removedPartitionId !== undefined) {
          removedPartitions.push({ partitionId: removedPartitionId, topic })
        }

        if (sortedAssignedTopics[topic].length === 0) {
          delete assignment[memberId][topic]
        }

        if (partitionsRemovedCount > partitionsToRemove) {
          break
        }
      }
    }
  }

  return removedPartitions
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
  getUnassignedPartitions,
  getMemberAssignedPartitionCount,
  unloadOverloadedMembers,
  hasImbalance,
}
