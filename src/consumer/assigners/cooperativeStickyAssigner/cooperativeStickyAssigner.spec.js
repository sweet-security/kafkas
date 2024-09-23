const {
  extractTopicPartitions,
  hasImbalance,
  unloadOverloadedMembers,
  getUnassignedPartitions,
  assignUnassignedPartitions,
  calculateAvgPartitions,
} = require('./utils')

describe('CooperativeSticky', () => {
  const members = [
    { memberId: 'member0' },
    { memberId: 'member1' },
    { memberId: 'member2' },
    { memberId: 'member3' },
    { memberId: 'member4' },
    { memberId: 'member5' },
  ]
  const topics = ['topic1', 'topic2', 'topic3']
  const allTopicsPartitions = {
    topic1: [0, 1, 2, 3, 4, 5],
    topic2: [0, 1, 2, 3, 4, 5],
    topic3: [0, 1, 2, 3, 4, 5],
  }

  let assignment, topicsPartitions

  beforeEach(() => {
    topicsPartitions = topics.flatMap(topic => {
      const partitionsMetadata = allTopicsPartitions[topic]
      return partitionsMetadata.map(partitionMetadata => ({
        topic,
        partitionId: partitionMetadata,
      }))
    })

    assignment = {}

    for (const member of members) {
      assignment[member.memberId] = {}
    }
  })

  it('avgPartitions', async () => {
    const avgPartitions = calculateAvgPartitions(topicsPartitions.length, members.length)
    expect(avgPartitions).toEqual(3)
    expect(hasImbalance(assignment, avgPartitions)).toBeFalsy()
  })

  it('first assignment no imbalance', async () => {
    const avgPartitions = calculateAvgPartitions(topicsPartitions.length, members.length)
    expect(hasImbalance(assignment, avgPartitions)).toBeFalsy()
  })

  it('first assignment unassigned partitions', async () => {
    const unassignedPartitions = getUnassignedPartitions(assignment, topicsPartitions)
    expect(unassignedPartitions).toEqual(topicsPartitions)
  })

  it('first assignment sanity', async () => {
    const unassignedPartitions = getUnassignedPartitions(assignment, topicsPartitions)
    assignUnassignedPartitions(assignment, unassignedPartitions, members)
    for (const member of members) {
      expect(assignment[member.memberId]).toBeDefined()
    }
  })

  it('first assignment is balanced', async () => {
    const avgPartitions = calculateAvgPartitions(topicsPartitions.length, members.length)
    const unassignedPartitions = getUnassignedPartitions(assignment, topicsPartitions)
    assignUnassignedPartitions(assignment, unassignedPartitions, members)
    for (const member of members) {
      const assignedMemberPartitions = assignment[member.memberId]
      let assignedMemberPartitionsCount = 0

      for (const topic in assignedMemberPartitions) {
        assignedMemberPartitionsCount += assignedMemberPartitions[topic].length
      }

      expect(assignedMemberPartitionsCount).toEqual(avgPartitions)
    }
  })
})
