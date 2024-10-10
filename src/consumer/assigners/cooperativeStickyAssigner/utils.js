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
  let [
    topicWithMostPartitions,
    partitionsOfTopicWithMostPartitions,
  ] = maxBy(currentMemberAssignment, [
    ([_, assignedTopicPartitions]) => assignedTopicPartitions.length,
  ])

  const topicsWithMostPartitionsInMemberAssigment = filter(
    currentMemberAssignment,
    ([_, assignedTopicPartitions]) =>
      assignedTopicPartitions.length === partitionsOfTopicWithMostPartitions.length
  )

  // If more than one topic with same partitions amount, remove partition from topic that removedPartitions has less of
  // e.g: removedPartitions = {a: [1,2], b: [1]}, memberAssignment = {a: [3,4], b: [3,4]} ==> removedPartitions = {a: [1,2], b: [1, 3]}, memberAssignment = {a: [3,4], b: [4]}
  if (topicsWithMostPartitionsInMemberAssigment.length > 1) {
    ;[
      topicWithMostPartitions,
      partitionsOfTopicWithMostPartitions,
    ] = minBy(topicsWithMostPartitionsInMemberAssigment, [
      ([topic, _]) => removedPartitions[topic].length ?? INT_32_MAX_VALUE,
    ])
  }
  return [topicWithMostPartitions, partitionsOfTopicWithMostPartitions]
}

const unloadOverloadedMembers = (assignment, avgPartitions) => {
  const removedPartitions = {}
  for (const memberId in assignment) {
    const memberAssignedPartitionCount = getMemberAssignedPartitionCount(assignment, memberId)
    const partitionsToRemove = memberAssignedPartitionCount - avgPartitions

    if (partitionsToRemove === 0) {
      return
    }

    let partitionsRemovedCount = 0
    let currentMemberAssignment
    while (partitionsRemovedCount < partitionsToRemove) {
      currentMemberAssignment = Object.entries(assignment[memberId])

      let [
        topicWithMostPartitions,
        partitionsOfTopicWithMostPartitions,
      ] = getTopicWithMostPartitionsFromMemberAssignment(currentMemberAssignment, removedPartitions)

      const removedPartitionId = partitionsOfTopicWithMostPartitions.pop()
      partitionsRemovedCount++

      if (removedPartitionId !== undefined) {
        if (!removedPartitions[topicWithMostPartitions]) {
          removedPartitions[topicWithMostPartitions] = [removedPartitionId]
        } else {
          removedPartitions[topicWithMostPartitions].push(removedPartitionId)
        }
      }

      if (partitionsOfTopicWithMostPartitions.length === 0) {
        delete assignment[memberId][topicWithMostPartitions]
      }
    }
  }

  return removedPartitions
}

const assignUnassignedPartitions = (assignment, unassignedPartitionsByTopics, members) => {
  while (Object.keys(unassignedPartitionsByTopics).length) {
    const [
      topicWithMostUnassignedPartitions,
      unassignedPartitionsOfTopicWithMostUnassignedPartitions,
    ] = maxBy(
      Object.entries(unassignedPartitionsByTopics),
      ([_, unassignedPartitions]) => unassignedPartitions.length
    )

    const partitionToAssign = unassignedPartitionsOfTopicWithMostUnassignedPartitions.pop()

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

    if (unassignedPartitionsOfTopicWithMostUnassignedPartitions.length === 0) {
      delete unassignedPartitionsByTopics[memberWithLeastPartitions]
    }
  }
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
    ...Object.keys(membersByAssignedPartitionsCount).map(parseInt)
  )

  if (membersByAssignedPartitionsCount[leastAssignedPartitionsCount].length === 1) {
    return membersByAssignedPartitionsCount[leastAssignedPartitionsCount][0]
  }
  // among the members with the least assigned partitions, find the one with the least amount of partitions assigned to it from the topic with most unassigned partitions
  return minBy(
    membersByAssignedPartitionsCount[leastAssignedPartitionsCount],
    member => assignment[member.memberId][topicWithMostUnassignedPartitions].length
  )
}

const getMemberAssignedPartitionCount = (assignment, memberId) => {
  return Object.values(assignment[memberId] || {}).flat().length
}

const getUnassignedPartitionsByTopics = (currentAssignment, topicsPartitions) => {
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
  getUnassignedPartitionsByTopics,
  unloadOverloadedMembers,
  hasImbalance,
  assignUnassignedPartitions,
}
