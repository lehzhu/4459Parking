
## RAFT basics

Reference video: https://youtu.be/vYp4LYbnnW8?si=moDL_yhfDs1HxWtS

Cluster membership is pre-defined; all members know the members in the cluster ahead of time

### Member roles

All members start as followers, taking orders from a leader.

#### If all members start as followers, how do we get a leader?
Each follower has a heartbeat timeout. If it has not received a heartbeat from the leader before the timeout, it puts itself forward as a candidate for the leader position.

#### How does a candidate become a leader?
When a member puts itself as a candidate, it votes for itself and also asks all other members for their vote. If the candidate receives more than half of the possible votes (at least (total members / 2) + 1) it becomes the leader and starts sending heartbeats to maintain the other members as followers.

#### How do you get a vote from other members?
When a follower becomes a candidate, it asks all other members for their vote. Followers will say yes to this request, but other candidates will say no. If a follower has already provided its vote for the current term, it will say no.

#### How does a candidate become a follower?
If a candidate receives a heartbeat from another member, it becomes a follower. Heartbeats are only sent by the leader, and a member can only become a leader if they have gotten majority vote. Since a candidate has received a heartbeat, it knows it lost the vote.

#### What if an old leader sent a heartbeat after a follower hit their heartbeat timeout?
When the heartbeat timeout occurs, a follower becomes a candidate, but also increments the 'term'. Heartbeats contain this term, and if a member receives a heartbeat with a term lower than their own, they tell the old leader, and the old leader becomes a follower. The term acts as a time control in RAFT. All messages contain the senders term. If a message is received with a term greater than your own, you need to update. If a message is received with a term less than your own, you respond back with your own term, letting the sender know they need to update.

#### What if many members become candidates and no one gets majority?
We try to avoid this by using a randomized timeout, but if this does happen, we wait for a timeout to occur on one of the candidates. They increment their term and restart the voting process. Candidates with a lower term will become followers and vote for them since they are behind. This is why randomized timeouts are crucial to allowing progress.

#### Election correctness

- Safety: Since we require a majority vote to become leader, and a member can only vote once per term, there can only be one leader for a given term. The voting action must be logged in persistent storage, so that if the member crashes after providing a vote, it won't provide another vote for the same term after restarting.
- Progress: If all members have the same timeout duration, no candidate would get a majority vote, and there would be no progress. We can use randomness in the timeout duration, or some ranking system among the members to fix this issue. Random timeout durations is simplest.

### Normal operation

Now that we have a leader and followers, we can execute commands.

Clients send commands to the leader.
Leader appends the command to its logs.
Leader sends AppendEntries RPC to followers.
Once an entry is committed, the leader executes the command, and returns the result to the client.
Leader notifies followers that the command has been committed, and they execute the command as well.

TODO: fill this out