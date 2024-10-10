const { MemberMetadata, MemberAssignment } = require('../../assignerProtocol')
const {
  extractTopicPartitions,
  calculateAvgPartitions,
  hasImbalance,
  unloadOverloadedMembers,
  getUnassignedPartitionsByTopics,
  assignUnassignedPartitions,
} = require('./utils')

/**
 * CooperativeStickyAssigner
 * @type {import('types').PartitionAssigner}
 */
module.exports = ({ cluster }) => ({
  name: 'CooperativeStickyAssigner',
  version: 0,
  async assign({ members, topics, currentAssignment }) {
    const assignment = {}

    // // Initialize assignment map for each member
    for (const member of members) {
      assignment[member.memberId] = currentAssignment[member.memberId] ?? {}
    }

    // Step 0: Fetch current partition metadata for topics
    const topicsPartitions = extractTopicPartitions(topics, cluster)

    // Step 1: Detect imbalance and redistribute partitions if necessary
    const avgPartitions = calculateAvgPartitions(topicsPartitions.length, members.length)
    if (hasImbalance(assignment, avgPartitions)) {
      unloadOverloadedMembers(assignment, avgPartitions)
    }

    // Step 2: If not already assigned, distribute using round-robin balancing
    const unassignedPartitionsByTopics = getUnassignedPartitionsByTopics(assignment, topicsPartitions)
    assignUnassignedPartitions(assignment, unassignedPartitionsByTopics, members)

    return encodeAssignment(assignment, this.version)
  },

  protocol({ topics }) {
    return {
      name: this.name,
      metadata: MemberMetadata.encode({
        version: this.version,
        topics: topics,
      }),
    }
  },
})

const encodeAssignment = (assignment, version) => {
  return Object.keys(assignment).map(memberId => ({
    memberId,
    memberAssignment: MemberAssignment.encode({
      userData: Buffer.alloc(0),
      version,
      assignment: assignment[memberId],
    }),
  }))
}
