use crate::aggregator::Aggregator;
use crate::config::Committee;
use crate::consensus::{ConsensusMessage, Round};
use crate::error::{ConsensusError, ConsensusResult};
use crate::leader::LeaderElector;
use crate::mempool::MempoolDriver;
use crate::messages::{Block, Timeout, Vote, QC, TC,Time1,TC1,Time2,TC2,help};
use crate::messages::{Precommit,Commit,Certificates};
use crate::proposer::ProposerMessage;
use crate::synchronizer::Synchronizer;
use crate::timer::Timer;
use async_recursion::async_recursion;
use bytes::Bytes;
use rand::seq::SliceRandom;
use tokio::sync::Notify;
use tokio::time::{Duration, Instant, Sleep};
use std::sync::Arc;
use crypto::Hash as _;
use crypto::Digest;
use bincode;
use crypto::{PublicKey, SignatureService};
use log::{debug, error, info, warn};
// use hex;
use network::SimpleSender;
use std::cmp::max;
use std::collections::{HashMap, HashSet, VecDeque}; 
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use std::pin::Pin;
#[cfg(test)]
#[path = "tests/core_tests.rs"]
pub mod core_tests;
pub struct Timer1 {
    start_time: Option<Instant>,
    round: Round,
    timeout_duration: Duration,
}

impl Timer1 {
    pub fn new() -> Self {
        Self {
            start_time: None,
            round: 0,
            timeout_duration: Duration::from_secs(2),
        }
    }
    
    // 启动计时器
    pub fn start(&mut self, round: Round) {
        info!("Starting Timer1 for round {}", round);
        self.round = round;
        self.start_time = Some(Instant::now());
    }
    
    // 停止计时器
    pub fn stop(&mut self) {
        if self.start_time.is_some() {
            info!("Stopping Timer1 for round {}", self.round);
            self.start_time = None;
        }
    }
    
    // 检查是否活跃
    pub fn is_active(&self) -> bool {
        self.start_time.is_some()
    }
    
    // 🔥 检查是否超时
    pub fn is_timeout(&self) -> bool {
        if let Some(start_time) = self.start_time {
            start_time.elapsed() >= self.timeout_duration
        } else {
            false
        }
    }
    
    // 获取轮次
    pub fn get_round(&self) -> Round {
        self.round
    }
}

pub struct Timer2 {
    start_time: Option<Instant>,
    round: Round,
    timeout_duration: Duration,
}

impl Timer2 {
    pub fn new() -> Self {
        Self {
            start_time: None,
            round: 0,
            timeout_duration: Duration::from_secs(2),
        }
    }
    
    // 启动计时器
    pub fn start(&mut self, round: Round) {
        info!("Starting Timer2 for round {}", round);
        self.round = round;
        self.start_time = Some(Instant::now());
    }
    
    // 停止计时器
    pub fn stop(&mut self) {
        if self.start_time.is_some() {
            info!("Stopping Timer2 for round {}", self.round);
            self.start_time = None;
        }
    }
    
    // 检查是否活跃
    pub fn is_active(&self) -> bool {
        self.start_time.is_some()
    }
    
    // 🔥 检查是否超时
    pub fn is_timeout(&self) -> bool {
        if let Some(start_time) = self.start_time {
            start_time.elapsed() >= self.timeout_duration
        } else {
            false
        }
    }
    
    // 获取轮次
    pub fn get_round(&self) -> Round {
        self.round
    }
}
pub struct Core {
    name: PublicKey,
    committee: Committee,
    store: Store,
    signature_service: SignatureService,
    leader_elector: LeaderElector,
    mempool_driver: MempoolDriver,
    synchronizer: Synchronizer,
    rx_message: Receiver<ConsensusMessage>,
    rx_loopback: Receiver<Block>,
    tx_proposer: Sender<ProposerMessage>,
    tx_commit: Sender<Block>,
    round: Round,
    last_voted_round: Round,
    last_committed_round: Round,
    high_qc: QC,
    timer: Timer,
    aggregator: Aggregator,
    network: SimpleSender,
    qcs: Vec<QC>,
    votes: Vec<Vote>,
    blocks: Vec<Block>,
    precommits: HashMap<Round, HashMap<PublicKey, Precommit>>,
    commit_notify: Arc<Notify>,
    time1: Timer1,
    time2: Timer2,
    timeout_duration: Duration,
    time1_messages: HashMap<Round, Vec<Time1>>,
    time2_messages: HashMap<Round, Vec<Time2>>,
}

impl Core {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        signature_service: SignatureService,
        store: Store,
        leader_elector: LeaderElector,
        mempool_driver: MempoolDriver,
        synchronizer: Synchronizer,
        timeout_delay: u64,
        rx_message: Receiver<ConsensusMessage>,
        rx_loopback: Receiver<Block>,
        tx_proposer: Sender<ProposerMessage>,
        tx_commit: Sender<Block>,
    ) {
        info!(
            "core spawn successful",
        );
        tokio::spawn(async move {
            Self {
                name,
                committee: committee.clone(),
                signature_service,
                store,
                leader_elector,
                mempool_driver,
                synchronizer,
                rx_message,
                rx_loopback,
                tx_proposer,
                tx_commit,
                round: 1,
                last_voted_round: 0,
                last_committed_round: 1,
                high_qc: QC::genesis(),
                timer: Timer::new(timeout_delay),
                aggregator: Aggregator::new(committee),
                network: SimpleSender::new(),
                qcs: Vec::new(),
                votes: Vec::new(),
                blocks: Vec::new(),
                // 初始化预提交消息存储
                precommits: HashMap::new(),
                commit_notify: Arc::new(Notify::new()), 
                time1: Timer1::new(),
                time2: Timer2::new(),
                timeout_duration: Duration::from_secs(2),
                time1_messages: HashMap::new(),
                time2_messages: HashMap::new(),
            }
            .run()
            .await
        });
    }
    async fn store_block(&mut self, block: &Block) {
        info!("Storing block {:?}", block);
        // 检查是否已存在相同哈希的区块
        if !self.blocks.iter().any(|b| b.digest() == block.digest()) {
            self.blocks.push(block.clone());
        }
    }

    async fn get_block_by_hash(&self, hash: &Digest) -> ConsensusResult<Option<Block>> {
        // 在内存向量中查找区块
        let block = self.blocks.iter()
            .find(|b| b.digest() == *hash)
            .cloned();
            
        if block.is_some() {
            info!("Found block with hash {}", hash);
        } else {
            info!("Block with hash {} not found", hash);
        }
        
        Ok(block)
    }
    async fn has_block(&self, hash: &Digest) -> ConsensusResult<bool> {
        let exists = self.blocks.iter().any(|b| b.digest() == *hash);
        info!("Block with hash {} exists: {}", hash, exists);
        Ok(exists)
    }
    async fn make_vote(&mut self, block: &Block) -> Option<Vote> {
        // 验证区块签名
        if let Err(_) = block.verify(&self.committee) {
            return None;
        }
        
        // 创建投票
        Some(Vote::new(block, self.name, self.signature_service.clone()).await)
    }

    async fn commit(&mut self, blocks: Vec<Block>) -> ConsensusResult<()> {
        for block in blocks {
            let block_digest = block.digest(); // 使用digest()方法获取摘要
            let block_round = block.round;
            // if block.round > self.last_committed_round {
            //     self.last_committed_round = block.round;
            // }
            
            if !block.payload.is_empty() {
                info!("Committed {}", block);

                #[cfg(feature = "benchmark")]
                for x in &block.payload {
                    info!("Committed {} -> {:?}", block, x);
                }
            }
            info!("Committed {:?}", block);
            
            if let Err(e) = self.tx_commit.send(block).await {
                warn!("Failed to send block through the commit channel: {}", e);
            }
            if self.last_committed_round >= 30 {
                let cleanup_threshold = self.last_committed_round - 30;
                self.blocks.retain(|block| block.round > cleanup_threshold);
            }
            // let initial_count = self.blocks.len();
            // self.blocks.retain(|b: &Block| b.digest() != block_digest);
            // let removed_count = initial_count - self.blocks.len();
            self.aggregator.cleanup_by_round_and_digest(&block_round, &block_digest);
            
        }
        Ok(())
    }
    async fn send_block_to_requester(&mut self, block: &Block, requester: &PublicKey) {
        info!("Sending block {} to requester {}", block.digest(), requester);
        
        // 获取请求者的网络地址
        let requester_address = self
            .committee
            .broadcast_addresses(&self.name)
            .into_iter()
            .find(|(author, _)| author == requester)
            .map(|(_, address)| address);
        
        if let Some(address) = requester_address {
            // 序列化区块消息
            let message = bincode::serialize(&ConsensusMessage::Propose(block.clone()))
                .expect("Failed to serialize block for help response");
            
            // 点对点发送给请求者
            self.network
                .broadcast(vec![address], Bytes::from(message))
                .await;
            
            info!("Block sent to requester {}", requester);
        } else {
            warn!("No address found for requester {}", requester);
        }
    }
    async fn broadcast_vote_vector(&mut self) {
        if self.votes.is_empty() {
            info!("No votes to broadcast");
            return;
        }
        
        info!("Broadcasting vector of {} votes", self.votes.len());
        let votes = self.votes.clone();    
        for vote in &votes {
            let _ = self.handle_vote(vote).await;
        }
        // 广播投票向量
        let addresses = self
            .committee
            .broadcast_addresses(&self.name)
            .into_iter()
            .map(|(_, x)| x)
            .collect();
        
        let message = bincode::serialize(&ConsensusMessage::VoteVector(self.votes.clone()))
            .expect("Failed to serialize vote vector");
        
        self.network
            .broadcast(addresses, Bytes::from(message))
            .await;
        
        // 广播后清空投票向量
        self.votes.clear();
        info!("Vote vector cleared after broadcasting");
    }

    async fn broadcast_certificates(&mut self, certificates: &Certificates) {
        info!("Broadcasting certificates for round {}", certificates.round);
        
        // 广播证书集合
        let addresses = self
            .committee
            .broadcast_addresses(&self.name)
            .into_iter()
            .map(|(_, x)| x)
            .collect();
        
        let message = bincode::serialize(&ConsensusMessage::Certificates(certificates.clone()))
            .expect("Failed to serialize certificates");
        
        self.network
            .broadcast(addresses, Bytes::from(message))
            .await;
    }

    // 修改：广播precommit消息的方法，使用Precommit结构体
    async fn broadcast_precommit(&mut self, certificates: &Certificates, round: Round) {
        info!("Broadcasting precommit for round {}", round);
        
        // 创建Precommit对象
        let precommit = Precommit {
            certificates: certificates.clone(),
            round,
            author: self.name,
            // 这里可以添加签名如果需要
        };
        
        let addresses = self
            .committee
            .broadcast_addresses(&self.name)
            .into_iter()
            .map(|(_, x)| x)
            .collect();
        
        let message = bincode::serialize(&ConsensusMessage::Precommit(precommit))
            .expect("Failed to serialize precommit message");
        
        self.network
            .broadcast(addresses, Bytes::from(message))
            .await;
    }

    // 修改：广播commit消息的方法
    async fn broadcast_commit(&mut self, certificates: &Certificates, round: Round) {
        info!("Broadcasting commit for round {}", round);
        
        // 创建Commit对象
        let commit = Commit {
            certificates: certificates.clone(),
            round,
        };
        
        let addresses = self
            .committee
            .broadcast_addresses(&self.name)
            .into_iter()
            .map(|(_, x)| x)
            .collect();
        
        let message = bincode::serialize(&ConsensusMessage::Commit(commit))
            .expect("Failed to serialize commit message");
        
        self.network
            .broadcast(addresses, Bytes::from(message))
            .await;
    }
    async fn handle_vote_vector(&mut self, votes: &Vec<Vote>) -> ConsensusResult<()> {
        info!("Processing vote vector with {} votes", votes.len());
        
        // 逐一处理投票
        for vote in votes {
            self.handle_vote(vote).await?;
        }
        
        Ok(())
    }

    #[async_recursion]
    async fn handle_vote(&mut self, vote: &Vote) -> ConsensusResult<()> {
        info!("Processing {:?}", vote);
        
        // 验证投票
        vote.verify(&self.committee)?;
        
        // 添加投票并尝试合成证书
        if let Some(qc) = self.aggregator.add_vote(vote.clone())? {
            info!("Assembled {:?}", qc);
            
            // 将新证书添加到证书集合中
            self.qcs.push(qc);
        }
        
        Ok(())
    }
    async fn broadcast_help_message(&mut self, help: help) -> ConsensusResult<()> {
        info!("Broadcasting help request for block {}", help.hash);
        
        let addresses: Vec<_> = self
            .committee
            .broadcast_addresses(&self.name)
            .into_iter()
            .map(|(_, x)| x)
            .collect();
        
        let selected_addresses: Vec<_> = if addresses.len() <= 3 {
            addresses  // 这里移动了addresses
        } else {
            let hash_bytes = help.hash.as_ref();
            let mut selected = Vec::new();
            let addr_len = addresses.len();  // 先保存长度
            
            // 使用哈希的不同部分作为随机种子
            for i in 0..3 {
                let seed = if i < hash_bytes.len() {
                    hash_bytes[i] as usize
                } else {
                    (hash_bytes[i % hash_bytes.len()] as usize) * (i + 1)
                };
                
                let mut index = seed % addr_len;
                
                // 避免重复选择
                while selected.iter().any(|addr| addr == &addresses[index]) {
                    index = (index + 1) % addr_len;  // 使用保存的长度
                }
                
                selected.push(addresses[index].clone());  // 克隆而不是移动
            }
            
            selected
        };
        
        info!("Selected {} nodes out of {} available for help request", 
               selected_addresses.len(), 
               self.committee.broadcast_addresses(&self.name).len());  // 重新获取长度
        
        let message = bincode::serialize(&ConsensusMessage::help(help))
            .expect("Failed to serialize help message");
    
        self.network
            .broadcast(selected_addresses, Bytes::from(message))
            .await;
    
        Ok(())
    }
    #[async_recursion]
    async fn handle_proposal(&mut self, block: &Block) -> ConsensusResult<()> {
        let digest = block.digest();
        if self.blocks.iter().any(|b| b.round == block.round && b.author == block.author) {
            info!("Ignoring duplicate block from author {} for round {} (already have one)", 
                   block.author, block.round);
            return Ok(());
        }//检查是否一轮广播多个块
        info!("Processing proposal {:?}", block);
        
        // 验证区块
        block.verify(&self.committee)?;
        
        // 存储区块
        self.store_block(block).await;
        
        // 检查是否有区块的数据，如果没有则等待mempool同步
        // if !self.mempool_driver.verify(block.clone()).await? {
        //     debug!("Processing of {} suspended: missing payload", digest);
        //     return Ok(());
        // }

        // 如果是本节点创建的新区块，广播它
        // if block.round == self.round && block.author == self.name {
        //     debug!("Broadcasting our new block for round {}", block.round);
        //    self.broadcast_block(block).await;
        // }
        
        // 为区块生成投票但不广播，而是存储到votes向量中
        if let Some(vote) = self.make_vote(block).await {
            info!("Created vote for block {} and stored it", block.round);
            // 将投票存入votes向量而非立即广播
            if let Err(e) = self.handle_vote(&vote).await {
                warn!("Failed to handle vote: {}", e);
            }
            self.votes.push(vote);
            
            // 检查是否处理了足够多的区块以进入新轮次
            let blocks_in_round = self.blocks.iter()
                .filter(|b| b.round == self.round)
                .count();
            
            // 如果处理的区块数量超过委员会2/3，进入新轮次
            if blocks_in_round > (2 * self.committee.size() / 3) {
                info!("Processed over 2/3 blocks for round {}, moving to next round", self.round);
                
                // 更新轮次
                self.round += 1;
                info!("Moved to round {}", self.round);
                
                // 生成并广播新区块
                self.generate_block().await;
                
                // 广播投票向量
                self.broadcast_vote_vector().await;
                
                // 如果是leader，额外广播证书集合
                // if self.name == self.leader_elector.get_leader(self.round - 1) {
                //     debug!("As leader, broadcasting certificates for round {}", self.round - 1);
                //     let certificates = Certificates::new(self.qcs.clone(), self.round - 1);
                //     self.broadcast_certificates(&certificates).await;
                info!("Broadcasting certificates for round {} from node {}", self.round - 1, self.name);
                let certificates = Certificates::new(
                    self.qcs.clone(), 
                    self.round - 1, 
                    self.name  // 添加自己的公钥作为author
                );
                self.broadcast_certificates(&certificates).await;
                
                self.time1.start(self.round - 1);
            }
        }
        
        Ok(())
    }

    // 修改处理证书集合的方法，改为发送precommit消息
    async fn handle_certificates(&mut self, certificates: &Certificates) -> ConsensusResult<()> {
        info!("Processing certificates for round {} from author {}", 
               certificates.round, certificates.author);
        if self.last_committed_round > certificates.round {
                info!("Skipping certificates: already committed round {} > certificates round {}", 
                       self.last_committed_round, certificates.round);
                return Ok(()); 
            }
        // 验证author是否为certificates.round轮次的leader
        let expected_leader = self.leader_elector.get_leader(certificates.round);
        if certificates.author != expected_leader {
            info!("Ignoring certificates from non-leader author {} for round {}, expected leader: {}", 
                   certificates.author, certificates.round, expected_leader);
            return Ok(()); // 直接返回，不处理
        }
        if self.time1.is_active() && self.time1.get_round() == certificates.round {
            info!("Stopping Timer1 for round {} as certificates received", certificates.round);
            self.time1.stop();
        }
        info!("Certificates from valid leader {} for round {}", 
               certificates.author, certificates.round);
            for qc in &certificates.certificates {
                // 验证证书
                qc.verify(&self.committee)?;
            }
        let mut missing_blocks = Vec::new();
        for qc in &certificates.certificates {
            if self.has_block(&qc.hash).await? {
            } else {
                missing_blocks.push(qc.hash.clone());
            }
        }
        if !missing_blocks.is_empty() {
            info!("Requesting {} missing blocks", missing_blocks.len());
                
            for hash in &missing_blocks {
                let help_msg = help {
                    hash: hash.clone(),
                    author: self.name.clone(),
                };
                self.broadcast_help_message(help_msg).await?;
            }
        }
        // 验证所有证书是否有效
        
        // 发送precommit消息而不是直接提交
        info!("Broadcasting precommit for certificates in round {}", certificates.round);
        self.broadcast_precommit(certificates, certificates.round).await;
        let precommit = Precommit {
            certificates: certificates.clone(),
            round:certificates.round,
            author: self.name,
            // 这里可以添加签名如果需要
        };
        if let Err(e) = self.handle_precommit(&precommit).await {
            warn!("Failed to handle precommit: {}", e);
        }
        Ok(())
    }

    // 修改：处理precommit消息的方法，使用Precommit结构体
    async fn handle_precommit(&mut self, precommit: &Precommit) -> ConsensusResult<()> {
        let round = precommit.round;
        let sender = precommit.author;
        if self.last_committed_round > round {
            info!("Skipping precommit: already committed round {} > precommit round {}", 
                   self.last_committed_round, round);
            return Ok(()); 
        }
        info!("Processing precommit from {} for round {}", sender, round);
        
        // 验证发送者
        // if !self.committee.exists(&sender) {
        //     return Err(ConsensusError::UnknownAuthority(sender));
        // }
        
        // 初始化round对应的precommits集合（如果不存在）
        if !self.precommits.contains_key(&round) {
            self.precommits.insert(round, HashMap::new());
        }
        if let Some(round_precommits) = self.precommits.get(&round) {
            if !round_precommits.is_empty() {
            // 获取第一个precommit作为参考
                let first_precommit = round_precommits.values().next().unwrap();
            
            // 检查当前precommit是否与已有的不同
            if precommit.certificates.digest() != first_precommit.certificates.digest()  {
                    if !self.time2.is_active() || self.time2.get_round() != round {
                        info!("Starting Timer2 for round {} due to conflicting precommits", round);
                        self.time2.start(round);
                    }
                info!("Detected conflicting precommit for round {}: different certificates", round);
                return Ok(());
                }
            }
        }// 🔥 如果检测到不同的precommit消息，启动Timer2
        // 存储precommit消息
        if let Some(round_precommits) = self.precommits.get_mut(&round) {
            round_precommits.insert(sender, precommit.clone());
            
            // 检查是否收到足够多的precommit消息（超过2/3委员会数量）
            if round_precommits.len() > 2 * self.committee.size() / 3 {
                info!("Received over 2/3 precommits for round {}, generating commit", round);
                
                // 使用第一个precommit中的证书集合（因为所有precommit应该包含相同的证书集合）
                if let Some((_, first_precommit)) = round_precommits.iter().next() {
                    // 提取需要的数据，避免同时可变借用self
                    let certificates = first_precommit.certificates.clone();
                    
                    // 第一个异步操作
                    self.broadcast_commit(&certificates, round).await;
                    
                    // 第二个异步操作
                    // self.process_commit_certificates(&certificates).await?;
                    if let Err(e) = self.handle_commit(&certificates, round).await {
                        warn!("Failed to handle commit: {}", e);
                    }
                    // 清理已处理的precommit消息
                    self.precommits.remove(&round);
                }
            }
        }
        
        Ok(())
    }
    async fn handle_commit(&mut self, certificates: &Certificates, round: Round) -> ConsensusResult<()> {
        info!("Processing commit for certificates in round {}", round);
        if self.time2.is_active() && self.time2.get_round() == round {
            info!("Stopping Timer2 for round {} as commit received", round);
            self.time2.stop();
        }
        if self.last_committed_round > certificates.round {
            info!("Skipping commit: already committed round {} > certificates round {}", 
                   self.last_committed_round, certificates.round);
            return Ok(()); 
        }//如果提交过，则直接返回
        // while self.last_committed_round < certificates.round {
        //     debug!("Waiting for last_committed_round ({}) to reach certificates.round ({})", 
        //            self.last_committed_round, certificates.round);
            
        //     // 异步等待通知
        //     self.commit_notify.notified().await;
        //     // 重新检查条件（可能被其他提交唤醒）
        //     if self.last_committed_round > certificates.round {
        //         debug!("Condition changed: already committed round {} > certificates round {}", 
        //                self.last_committed_round, certificates.round);
        //         return Ok(());
        //     }
        // }//若小于则异步等待相等 
        self.process_commit_certificates(certificates).await
    }

    // 处理证书提交的方法
    async fn process_commit_certificates(&mut self, certificates: &Certificates) -> ConsensusResult<()> {
        let mut blocks_to_commit = Vec::new();
        for qc in &certificates.certificates {
            if let Ok(Some(block)) = self.get_block_by_hash(&qc.hash).await {
                // info!("committed block:{}",block);
                blocks_to_commit.push(block);
            }
            else {
                info!("Missing block for certificate, waiting for synchronization {}",qc.hash);
                let help_msg = help {
                    hash: qc.hash.clone(),
                    author: self.name.clone(),
                };
                self.broadcast_help_message(help_msg.clone()).await ?;
                // return Ok(());
            }
        }
        
        // 提交这些区块
        if !blocks_to_commit.is_empty() {
            let _ = self.commit(blocks_to_commit).await;
        }
        
        // 清空自己的证书集合（可能与接收到的有重叠）
        for qc in &certificates.certificates {
            self.qcs.retain(|q| q.hash != qc.hash);
        }
        self.last_committed_round += 1;
        info!("committed round move to {}",self.last_committed_round);
        self.commit_notify.notify_waiters();//更新提交轮次并判断等待条件
        Ok(())
    }

    // 生成区块的函数
    async fn generate_block(&mut self) {
        info!("Generating block for round {}", self.round);
        debug!("Generating block for round {}", self.round);
        // 向提案者发送创建区块的消息
        self.tx_proposer
            .send(ProposerMessage::Make(self.round, QC::genesis(), None))
            .await
            .expect("Failed to send message to proposer");
        
        // 注意：实际的区块会通过rx_loopback回到Core，然后在handle_proposal中处理和广播
    }
    async fn handle_help(&mut self, help_msg: &help) -> ConsensusResult<()> {
        info!("Processing help request from {} for block {}", 
               help_msg.author, help_msg.hash);
        
        // 在本地blocks向量中查找请求的区块
        if let Ok(Some(block)) = self.get_block_by_hash(&help_msg.hash).await {
            info!("Found requested block {} for {}, sending to requester", 
                  help_msg.hash, help_msg.author);
            
            // 点对点发送区块给请求者
            self.send_block_to_requester(&block, &help_msg.author).await;
            
            info!("Successfully sent block {} to {}", 
                  help_msg.hash, help_msg.author);
        } else {
            info!("Don't have the requested block {} for {}", 
                   help_msg.hash, help_msg.author);
        }
        
        Ok(())
    }
    async fn handle_timer1_timeout(&mut self) -> ConsensusResult<()> {
        let round = self.time1.get_round();
        if self.last_committed_round>round{
            info!("Skipping Timer1 timeout: already committed round {} > current round {}", 
                   self.last_committed_round, round);
            return Ok(()); 
        }
        info!("Timer1 timeout for round {}", round);
        
        // 🔥 广播Time1消息，按照broadcast_commit的样式
        info!("Broadcasting Time1 message for round {}", round);
        
        let time1_msg = Time1::new(round, self.name);
        
        let addresses = self
            .committee
            .broadcast_addresses(&self.name)
            .into_iter()
            .map(|(_, x)| x)  // 🔥 照样提取地址
            .collect();
        
        let message = bincode::serialize(&ConsensusMessage::Time1(time1_msg))
            .expect("Failed to serialize Time1 message");  // 🔥 使用bincode::serialize
        
        self.network
            .broadcast(addresses, Bytes::from(message))
            .await;  // 🔥 直接await，不处理错误
        
        // 停止Timer1
        self.time1.stop();
        Ok(())
    }
    
    // Timer2超时处理函数
    async fn handle_timer2_timeout(&mut self) -> ConsensusResult<()> {
        let round = self.time2.get_round();
    if self.last_committed_round > round {
        info!("Skipping Timer2 timeout: already committed round {} > current round {}", 
               self.last_committed_round, round);
        return Ok(()); 
    }
    info!("Timer2 timeout for round {}", round);
    
    // 🔥 广播Time2消息
    info!("Broadcasting Time2 message for round {}", round);
    
    let time2_msg = Time2::new(round, self.name);
    
    let addresses = self
        .committee
        .broadcast_addresses(&self.name)
        .into_iter()
        .map(|(_, x)| x)
        .collect();
    
    let message = bincode::serialize(&ConsensusMessage::Time2(time2_msg))
        .expect("Failed to serialize Time2 message");
    
    self.network
        .broadcast(addresses, Bytes::from(message))
        .await;
    
    // 停止Timer2
    self.time2.stop();
    Ok(())
    }
    async fn handle_time1(&mut self, time1: &Time1) -> ConsensusResult<()> {
        info!("Processing Time1 message for round {} from author {}", 
               time1.round, time1.author);
        
        // 验证author是否是有效的委员会成员
        ensure!(
            self.committee.stake(&time1.author) > 0,
            ConsensusError::UnknownAuthority(time1.author)
        );
        
        // 🔥 使用HashMap按轮次收集Time1消息，类似precommit处理
        self.time1_messages
            .entry(time1.round)
            .or_insert_with(Vec::new)
            .push(time1.clone());
        
        let time1_count = self.time1_messages
            .get(&time1.round)
            .map(|v| v.len())
            .unwrap_or(0);
        
        let threshold = (2 * self.committee.size() / 3) + 1;
        
        info!("Collected {} Time1 messages for round {}, threshold: {}", 
               time1_count, time1.round, threshold);
        
        // 🔥 如果收集的Time1消息超过2/3阈值，广播TC1
        if time1_count > (2 * self.committee.size() / 3) {
            info!("Time1 messages exceed 2/3 threshold for round {}, broadcasting TC1", time1.round);
            
            // 广播TC1消息
            let tc1_msg = TC1::new(time1.round, self.name);
            
            let addresses = self
                .committee
                .broadcast_addresses(&self.name)
                .into_iter()
                .map(|(_, x)| x)
                .collect();
            
            let message = bincode::serialize(&ConsensusMessage::TC1(tc1_msg))
                .expect("Failed to serialize TC1 message");
            
            self.network
                .broadcast(addresses, Bytes::from(message))
                .await;
            
            info!("Broadcasted TC1 message for round {}", time1.round);
            
            // 🔥 使last_commit_round++
            self.last_committed_round += 1;
            self.time2.stop();
            info!("Incremented last_commit_round to {}", self.last_committed_round);
        }
        
        Ok(())
    }
    async fn handle_tc1(&mut self, tc1: &TC1) -> ConsensusResult<()> {
        info!("Processing TC1 message for round {} from author {}", 
               tc1.round, tc1.author);
        
        // 🔥 如果已经提交了更高的轮次，直接返回
        if self.last_committed_round > tc1.round {
            info!("Skipping TC1: already committed round {} > TC1 round {}", 
                   self.last_committed_round, tc1.round);
            return Ok(()); 
        }
        self.time1.stop();
        // 验证author是否是有效的委员会成员
        ensure!(
            self.committee.stake(&tc1.author) > 0,
            ConsensusError::UnknownAuthority(tc1.author)
        );
        
        // 🔥 直接更新last_committed_round
        self.last_committed_round += 1;
        info!("Received TC1 from {}, incremented last_committed_round to {}", 
               tc1.author, self.last_committed_round);
        
        info!("Processed TC1 message from {} for round {}, last_committed_round now: {}", 
              tc1.author, tc1.round, self.last_committed_round);
        
        Ok(())
    }
    async fn handle_time2(&mut self, time2: &Time2) -> ConsensusResult<()> {
        info!("Processing Time2 message for round {} from author {}", 
               time2.round, time2.author);
        
        // 验证author是否是有效的委员会成员
        ensure!(
            self.committee.stake(&time2.author) > 0,
            ConsensusError::UnknownAuthority(time2.author)
        );
        
        // 🔥 使用HashMap按轮次收集Time2消息
        self.time2_messages
            .entry(time2.round)
            .or_insert_with(Vec::new)
            .push(time2.clone());
        
        let time2_count = self.time2_messages
            .get(&time2.round)
            .map(|v| v.len())
            .unwrap_or(0);
        
        let threshold = (2 * self.committee.size() / 3) + 1;
        
        info!("Collected {} Time2 messages for round {}, threshold: {}", 
               time2_count, time2.round, threshold);
        
        // 🔥 如果收集的Time2消息超过2/3阈值，广播TC2
        if time2_count > (2 * self.committee.size() / 3) {
            info!("Time2 messages exceed 2/3 threshold for round {}, broadcasting TC2", time2.round);
            
            // 广播TC2消息
            let tc2_msg = TC2::new(time2.round, self.name);
            
            let addresses = self
                .committee
                .broadcast_addresses(&self.name)
                .into_iter()
                .map(|(_, x)| x)
                .collect();
            
            let message = bincode::serialize(&ConsensusMessage::TC2(tc2_msg))
                .expect("Failed to serialize TC2 message");
            
            self.network
                .broadcast(addresses, Bytes::from(message))
                .await;
            
            info!("Broadcasted TC2 message for round {}", time2.round);
            
            // 🔥 使last_committed_round++
            self.last_committed_round += 1;
            self.time2.stop();
            info!("Incremented last_committed_round to {}", self.last_committed_round);
        }
        
        Ok(())
    }
    async fn handle_tc2(&mut self, tc2: &TC2) -> ConsensusResult<()> {
        info!("Processing TC2 message for round {} from author {}", 
               tc2.round, tc2.author);
        
        // 🔥 如果已经提交了更高的轮次，直接返回
        if self.last_committed_round > tc2.round {
            info!("Skipping TC2: already committed round {} > TC2 round {}", 
                   self.last_committed_round, tc2.round);
            return Ok(()); 
        }
        self.time2.stop();
        // 验证author是否是有效的委员会成员
        ensure!(
            self.committee.stake(&tc2.author) > 0,
            ConsensusError::UnknownAuthority(tc2.author)
        );
        
        // 🔥 直接更新last_committed_round
        self.last_committed_round += 1;
        info!("Received TC2 from {}, incremented last_committed_round to {}", 
               tc2.author, self.last_committed_round);
        
        info!("Processed TC2 message from {} for round {}, last_committed_round now: {}", 
              tc2.author, tc2.round, self.last_committed_round);
        
        Ok(())
    }
    // 主循环
    pub async fn run(&mut self) {
        // 启动时生成第一个区块
        info!(
            "run successful",
        );
        self.generate_block().await;
        info!(
            "generate successful",
        );
        // 主循环处理消息
        loop {
            if self.time1.is_active() && self.time1.is_timeout() {
                info!("Timer1 timeout detected for round {}", self.time1.get_round());
                let _ = self.handle_timer1_timeout().await;
            }
            
            if self.time2.is_active() && self.time2.is_timeout() {
                info!("Timer2 timeout detected for round {}", self.time2.get_round());
                let _ = self.handle_timer2_timeout().await;
            }
            let result = tokio::select! {
                Some(message) = self.rx_message.recv() => match message {
                    ConsensusMessage::Propose(block) => self.handle_proposal(&block).await,
                    ConsensusMessage::Vote(vote) => self.handle_vote(&vote).await,
                    ConsensusMessage::Certificates(certificates) => self.handle_certificates(&certificates).await,
                    ConsensusMessage::VoteVector(votes) => self.handle_vote_vector(&votes).await,
                    // 修改处理新消息类型的匹配
                    ConsensusMessage::Precommit(precommit) => 
                        self.handle_precommit(&precommit).await,
                    ConsensusMessage::Commit(commit) => 
                        self.handle_commit(&commit.certificates, commit.round).await,
                    ConsensusMessage::Time1(time1) => self.handle_time1(&time1).await,
                    ConsensusMessage::TC1(tc1) => self.handle_tc1(&tc1).await,
                    ConsensusMessage::Time2(time2) => self.handle_time2(&time2).await,
                    ConsensusMessage::TC2(tc2) => self.handle_tc2(&tc2).await,
                    ConsensusMessage::help(help_msg) => self.handle_help(&help_msg).await,
                    _ => Ok(()) // 新建进程处理消息
                },
                Some(block) = self.rx_loopback.recv() => self.handle_proposal(&block).await,
            };
            
            match result {
                Ok(()) => (),
                Err(ConsensusError::StoreError(e)) => error!("{}", e),
                Err(ConsensusError::SerializationError(e)) => error!("Store corrupted. {}", e),
                Err(e) => warn!("{}", e),
            }
        }
    }
}


