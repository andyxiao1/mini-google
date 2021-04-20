package edu.upenn.cis.stormlite.distributed;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.eclipse.jetty.util.ConcurrentHashSet;

/**
 * Coordinator for tracking and reaching consensus
 *
 * Tracks both who voted and how many votes were recorded. If someone votes
 * twice, there is an exception. If enough votes are received to pass a
 * specified threshold, we assume consensus is reached.
 *
 */
public class ConsensusTracker {
	static Logger log = LogManager.getLogger(ConsensusTracker.class);

	AtomicInteger votesForEos = new AtomicInteger(0);

	/**
	 * Track the set of voters
	 */
	ConcurrentHashSet<String> voteReceivedFrom = new ConcurrentHashSet<String>();

	int votesNeeded;

	public ConsensusTracker(int votesNeeded) {
		this.votesNeeded = votesNeeded;
	}

	/**
	 * Add another vote towards consensus.
	 *
	 * @param voter Optional ID of the node / executor that voted, for tracking if
	 *              anyone is voting more than once!
	 *
	 * @return true == we have enough votes for consensus end-of-stream. false == we
	 *         don't yet have enough votes.
	 */
	public boolean voteForEos(String voter) {
		if (voter == null || voter.isEmpty()) {
			return false;
		}

		if (voteReceivedFrom.contains(voter)) {
			// throw new RuntimeException("Executor " + voter + " already voted EOS!");
			log.info("Executor " + voter + " already voted EOS!");
			return false;
		}

		int votes = votesForEos.incrementAndGet();
		voteReceivedFrom.add(voter);

		return votes >= votesNeeded;

		// int votes = votesForEos.incrementAndGet();

		// if (voter != null && !voter.isEmpty()) {
		// // This is to help you debug
		// if (voteReceivedFrom.contains(voter)) {
		// throw new RuntimeException("Executor " + voter + " already voted EOS!");
		// } else {
		// voteReceivedFrom.add(voter);
		// }
		// }

		// if (votes >= votesNeeded)
		// return true;
		// else
		// return false;
	}
}
