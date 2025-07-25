use crate::config::Committee;
use crate::consensus::Round;
use crypto::PublicKey;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;


pub type LeaderElector = RRLeaderElector;

pub struct RRLeaderElector {
    committee: Committee,
}

impl RRLeaderElector {
    pub fn new(committee: Committee) -> Self {
        Self { committee }
    }

    // pub fn get_leader(&self, round: Round) -> PublicKey {
    //     let mut keys: Vec<_> = self.committee.authorities.keys().cloned().collect();
    //     keys.sort();
    //     keys[round as usize % self.committee.size()]
    // }
    pub fn get_leader(&self, round: Round) -> PublicKey {
        let mut keys: Vec<_> = self.committee.authorities.keys().cloned().collect();
        keys.sort();
        
        // 使用 round 作为种子，确保相同 round 产生相同结果
        let mut rng = StdRng::seed_from_u64(round as u64);
        let random_index = (rng.gen::<u32>() as usize) % self.committee.size();
        
        keys[random_index]
    }
}
