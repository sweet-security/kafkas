const { AssignerProtocol } = require('../../../../index')
const {
  hasImbalance,
  unloadOverloadedMembers,
  getUnassignedPartitions,
  getMemberAssignedPartitionCount,
} = require('./utils')
const { minBy, cloneDeep } = require('lodash')

/**
 * CooperativeStickyAssigner
 * @type {import('types').PartitionAssigner}
 */
module.exports = ({ cluster }) => ({
  name: 'CooperativeStickyAssigner',
  version: 0,
  async assign({ members, topics, currentAssignment }) {
    const membersCount = members.length
    const assignment = cloneDeep(currentAssignment)

    // // Initialize assignment map for each member
    for (const member of members) {
      if (!assignment[member.memberId]) {
        assignment[member.memberId] = {}
      }
    }

    // Step 0: Fetch current partition metadata for topics
    const topicsPartitions = topics.flatMap(topic => {
      const partitionsMetadata = cluster.findTopicPartitionMetadata(topic)
      return partitionsMetadata.map(partitionMetadata => ({
        topic,
        partitionId: partitionMetadata.partitionId,
      }))
    })

    // Step 1: Detect imbalance and redistribute partitions if necessary
    const totalPartitions = topicsPartitions.length
    const avgPartitions = Math.ceil(totalPartitions / membersCount)
    if (hasImbalance(assignment, avgPartitions)) {
      unloadOverloadedMembers(assignment, avgPartitions)
    }
    // Step 2: If not already assigned, distribute using round-robin balancing
    const unassignedPartitions = getUnassignedPartitions(currentAssignment, topicsPartitions)
    for (const unassignedPartition of unassignedPartitions) {
      const memberWithLeastPartitions = minBy(members, member =>
        getMemberAssignedPartitionCount(currentAssignment, member.memberId)
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

    return encodeAssignment(assignment, this.version)
  },

  protocol({ topics }) {
    return {
      name: this.name,
      metadata: AssignerProtocol.MemberMetadata.encode({
        version: this.version,
        topics: topics,
      }),
    }
  },
})

const encodeAssignment = (assignment, version) => {
  return Object.keys(assignment).map(memberId => ({
    memberId,
    memberAssignment: AssignerProtocol.MemberAssignment.encode({
      userData: Buffer.alloc(0),
      version,
      assignment: assignment[memberId],
    }),
  }))
}
