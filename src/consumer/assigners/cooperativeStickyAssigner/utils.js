const hasImbalance = (assignment, avgPartitions) => {
  return Object.values(assignment).some(
    topicPartitions => Object.values(topicPartitions).flat().length > avgPartitions
  )
}

const unloadOverloadedMembers = (assignment, avgPartitions) => {
  const removedPartitions = []
  for (const memberId in assignment) {
    const partitionCount = getMemberAssignedPartitionCount(assignment, memberId)
    const partitionsToRemove = partitionCount - avgPartitions

    if (partitionsToRemove > 0) {
      let partitionsRemovedCount = 0
      while (partitionsRemovedCount < partitionsToRemove) {
        for (const topic in assignment[memberId]) {
          const removedPartitionId = assignment[memberId][topic].pop()
          if (removedPartitionId !== undefined) {
            removedPartitions.push({ partitionId: removedPartitionId, topic })
          }

          if (assignment[memberId][topic].length === 0) {
            delete assignment[memberId][topic]
          }
          partitionsRemovedCount++
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
  hasImbalance
}