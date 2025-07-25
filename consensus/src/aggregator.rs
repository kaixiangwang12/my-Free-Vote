use crate::config::{Committee, Stake};
use crate::consensus::Round;
use crate::error::{ConsensusError, ConsensusResult};
use crate::messages::{Timeout, Vote, QC, TC};
use crypto::Hash as _;
use crypto::{Digest, PublicKey, Signature};
use std::collections::{HashMap, HashSet};

#[cfg(test)]
#[path = "tests/aggregator_tests.rs"]
pub mod aggregator_tests;

pub struct Aggregator {
    committee: Committee,
    votes_aggregators: HashMap<Round, HashMap<Digest, Box<QCMaker>>>,
    timeouts_aggregators: HashMap<Round, Box<TCMaker>>,
    // completed_qcs: HashSet<(Round, Digest)>,  
}

impl Aggregator {
    pub fn new(committee: Committee) -> Self {
        Self {
            committee,
            votes_aggregators: HashMap::new(),
            timeouts_aggregators: HashMap::new(),
            // completed_qcs: HashSet::new(),
            
        }
    }

    pub fn add_vote(&mut self, vote: Vote) -> ConsensusResult<Option<QC>> {
        
        self.votes_aggregators
            .entry(vote.round)
            .or_insert_with(HashMap::new)
            .entry(vote.digest())
            .or_insert_with(|| Box::new(QCMaker::new()))
            .append(vote, &self.committee)
    }//需要修改合成证书后不接受后续投票，并及时清理。


    pub fn add_timeout(&mut self, timeout: Timeout) -> ConsensusResult<Option<TC>> {
        // TODO: A bad node may make us run out of memory by sending many timeouts
        // with different round numbers.

        // Add the new timeout to our aggregator and see if we have a TC.
        self.timeouts_aggregators
            .entry(timeout.round)
            .or_insert_with(|| Box::new(TCMaker::new()))
            .append(timeout, &self.committee)
    }
    pub fn cleanup(&mut self, round: &Round) {
        self.votes_aggregators.retain(|k, _| k >= round);
        self.timeouts_aggregators.retain(|k, _| k >= round);
    }
    pub fn cleanup_by_round_and_digest(&mut self, round: &Round, digest: &Digest) -> bool {
        if let Some(round_aggregators) = self.votes_aggregators.get_mut(round) {
            let removed = round_aggregators.remove(digest).is_some();
            
            // 如果该轮次没有任何聚合器了，删除整个轮次条目
            if round_aggregators.is_empty() {
                self.votes_aggregators.remove(round);
            }
            
            removed
        } else {
            false
        }
    }
}

struct QCMaker {
    weight: Stake,
    votes: Vec<(PublicKey, Signature)>,
    used: HashSet<PublicKey>,
}

impl QCMaker {
    pub fn new() -> Self {
        Self {
            weight: 0,
            votes: Vec::new(),
            used: HashSet::new(),
        }
    }

    /// Try to append a signature to a (partial) quorum.
    pub fn append(&mut self, vote: Vote, committee: &Committee) -> ConsensusResult<Option<QC>> {
        let author = vote.author;

        // Ensure it is the first time this authority votes.
        ensure!(
            self.used.insert(author),
            ConsensusError::AuthorityReuse(author)
        );

        self.votes.push((author, vote.signature));
        self.weight += committee.stake(&author);
        if self.weight >= committee.quorum_threshold() {
            self.weight = 0; // Ensures QC is only made once.
            return Ok(Some(QC {
                hash: vote.hash.clone(),
                round: vote.round,
                votes: self.votes.clone(),
            }));
        }
        Ok(None)
    }
}

struct TCMaker {
    weight: Stake,
    votes: Vec<(PublicKey, Signature, Round)>,
    used: HashSet<PublicKey>,
}

impl TCMaker {
    pub fn new() -> Self {
        Self {
            weight: 0,
            votes: Vec::new(),
            used: HashSet::new(),
        }
    }

    /// Try to append a signature to a (partial) quorum.
    pub fn append(
        &mut self,
        timeout: Timeout,
        committee: &Committee,
    ) -> ConsensusResult<Option<TC>> {
        let author = timeout.author;

        // Ensure it is the first time this authority votes.
        ensure!(
            self.used.insert(author),
            ConsensusError::AuthorityReuse(author)
        );

        // Add the timeout to the accumulator.
        self.votes
            .push((author, timeout.signature, timeout.high_qc.round));
        self.weight += committee.stake(&author);
        if self.weight >= committee.quorum_threshold() {
            self.weight = 0; // Ensures TC is only created once.
            return Ok(Some(TC {
                round: timeout.round,
                votes: self.votes.clone(),
            }));
        }
        Ok(None)
    }
}
