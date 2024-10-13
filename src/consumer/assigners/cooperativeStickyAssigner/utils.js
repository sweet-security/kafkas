const { maxBy, minBy, filter } = require('lodash')
const { INT_32_MAX_VALUE } = require('../../../constants')

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
  return Math.ceil(topicsPartitionsCount / membersCount)
}

const hasImbalance = (assignment, avgPartitions) => {
  return Object.values(assignment).some(
    topicPartitions => Object.values(topicPartitions).flat().length > avgPartitions
  )
}

// Find the topic with the largest partition count in the member assignment
const getTopicWithMostPartitionsFromMemberAssignment = (
  currentMemberAssignment,
  removedPartitions
) => {
  const topicsByPartitionsCount = {}

  for (const topic in currentMemberAssignment) {
    const topicPartitionCount = currentMemberAssignment[topic].length
    if (!topicsByPartitionsCount[topicPartitionCount]) {
      topicsByPartitionsCount[topicPartitionCount] = [topic]
    } else {
      topicsByPartitionsCount[topicPartitionCount].push(topic)
    }
  }

  const topicWithMostPartitionsPartitionCount = Math.max(
    ...Object.keys(topicsByPartitionsCount).map(partitionCount => parseInt(partitionCount))
  )

  if (topicsByPartitionsCount[topicWithMostPartitionsPartitionCount].length === 1) {
    return topicsByPartitionsCount[topicWithMostPartitionsPartitionCount][0]
  }

  // If more than one topic with same partitions amount, remove partition from topic that removedPartitions has less of
  // e.g: removedPartitions = {a: [1,2], b: [1]}, memberAssignment = {a: [3,4], b: [3,4]} ==> removedPartitions = {a: [1,2], b: [1, 3]}, memberAssignment = {a: [3,4], b: [4]}
  return minBy(topicsByPartitionsCount[topicWithMostPartitionsPartitionCount], [
    assignedTopic => removedPartitions[assignedTopic].length ?? INT_32_MAX_VALUE,
  ])
}

const getMemberIdToAssignPartitionTo = (members, assignment, topicWithMostUnassignedPartitions) => {
  const membersByAssignedPartitionsCount = {}

  for (const member of members) {
    const memberAssignedPartitionsCount = getMemberAssignedPartitionCount(
      assignment,
      member.memberId
    )
    if (!membersByAssignedPartitionsCount[memberAssignedPartitionsCount]) {
      membersByAssignedPartitionsCount[memberAssignedPartitionsCount] = [member]
    } else {
      membersByAssignedPartitionsCount[memberAssignedPartitionsCount].push(member)
    }
  }

  const leastAssignedPartitionsCount = Math.min(
    ...Object.keys(membersByAssignedPartitionsCount).map(partitionCount => parseInt(partitionCount))
  )

  if (membersByAssignedPartitionsCount[leastAssignedPartitionsCount].length === 1) {
    return membersByAssignedPartitionsCount[leastAssignedPartitionsCount][0]
  }
  // Amongst the members with the least assigned partitions, find the one with the least amount of partitions assigned to it from the topic with most unassigned partitions
  return minBy(
    membersByAssignedPartitionsCount[leastAssignedPartitionsCount],
    member => assignment[member.memberId][topicWithMostUnassignedPartitions].length
  )
}

const unloadOverloadedMembers = (assignment, avgPartitions) => {
  const removedPartitions = {}
  for (const memberId in assignment) {
    const memberAssignedPartitionCount = getMemberAssignedPartitionCount(assignment, memberId)

    let partitionsToRemove = memberAssignedPartitionCount - avgPartitions
    while (partitionsToRemove > 0) {
      const topicWithMostPartitions = getTopicWithMostPartitionsFromMemberAssignment(
        assignment[memberId],
        removedPartitions
      )

      const removedPartitionId = assignment[memberId][topicWithMostPartitions].pop()
      partitionsToRemove--

      if (removedPartitionId !== undefined) {
        if (!removedPartitions[topicWithMostPartitions]) {
          removedPartitions[topicWithMostPartitions] = [removedPartitionId]
        } else {
          removedPartitions[topicWithMostPartitions].push(removedPartitionId)
        }
      }

      if (assignment[memberId][topicWithMostPartitions].length === 0) {
        delete assignment[memberId][topicWithMostPartitions]
      }
    }
  }

  return removedPartitions
}

const assignUnassignedPartitions = (assignment, unassignedPartitionsByTopic, members) => {
  while (Object.keys(unassignedPartitionsByTopic).length) {
    const topicWithMostUnassignedPartitions = maxBy(
      Object.keys(unassignedPartitionsByTopic),
      topic => unassignedPartitionsByTopic[topic].length
    )

    const partitionToAssign = unassignedPartitionsByTopic[topicWithMostUnassignedPartitions].pop()

    const memberWithLeastPartitions = getMemberIdToAssignPartitionTo(
      members,
      assignment,
      topicWithMostUnassignedPartitions
    ).memberId

    if (!assignment[memberWithLeastPartitions][topicWithMostUnassignedPartitions]) {
      assignment[memberWithLeastPartitions][topicWithMostUnassignedPartitions] = [partitionToAssign]
    } else {
      assignment[memberWithLeastPartitions][topicWithMostUnassignedPartitions].push(
        partitionToAssign
      )
    }

    if (unassignedPartitionsByTopic[topicWithMostUnassignedPartitions].length === 0) {
      delete unassignedPartitionsByTopic[topicWithMostUnassignedPartitions]
    }
  }
}

const getMemberAssignedPartitionCount = (assignment, memberId) => {
  return Object.values(assignment[memberId] || {}).flat().length
}

const getUnassignedPartitionsByTopic = (currentAssignment, topicsPartitions) => {
  const assignedTopicPartitions = Object.values(currentAssignment)
  const unassignedPartitionsByTopic = {}
  for (const topicPartition of topicsPartitions) {
    const isPartitionAssigned = assignedTopicPartitions.some(assignedTopicPartition =>
      assignedTopicPartition[topicPartition.topic]?.includes(topicPartition.partitionId)
    )

    if (isPartitionAssigned) {
      continue
    }

    if (!unassignedPartitionsByTopic[topicPartition.topic]) {
      unassignedPartitionsByTopic[topicPartition.topic] = [topicPartition.partitionId]
    } else {
      unassignedPartitionsByTopic[topicPartition.topic].push(topicPartition.partitionId)
    }
  }

  return unassignedPartitionsByTopic
}

module.exports = {
  extractTopicPartitions,
  calculateAvgPartitions,
  getUnassignedPartitionsByTopic,
  unloadOverloadedMembers,
  hasImbalance,
  assignUnassignedPartitions,
}
