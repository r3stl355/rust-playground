use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::mpsc::{self, TryRecvError};
use std::time::Duration;
use std::{collections::HashMap, io::Write};

use rostengan_my::*;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum BroadcastPayload {
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
    Broadcast {
        message: usize,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: HashSet<usize>,
    },
    Gossip {
        messages: HashSet<usize>,
        also_sent_to: Vec<String>,
    },
    GossipOk,
}

struct BroadcastResponder {
    node_id: String,
    next_msg_id: usize,
    neighbours: Vec<String>,
    messages: HashSet<usize>,
    stop_sender: mpsc::Sender<()>,
    messages_to_gossip: HashMap<String, HashSet<usize>>,
    gossip_sent: HashMap<usize, (String, HashSet<usize>)>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InternalMessage {
    Gossip,
}

fn start_gossip(
    sender: mpsc::Sender<MessageEvent<BroadcastPayload, InternalMessage>>,
    stop_channel: mpsc::Receiver<()>,
) {
    std::thread::spawn(move || loop {
        std::thread::sleep(Duration::from_millis(100));

        // Exit the loop if signalled
        match stop_channel.try_recv() {
            Ok(_) | Err(TryRecvError::Disconnected) => {
                break;
            }
            Err(TryRecvError::Empty) => {}
        }
        sender
            .send(MessageEvent::Internal(InternalMessage::Gossip))
            .unwrap();
    });
}

impl Responder<BroadcastPayload, InternalMessage> for BroadcastResponder {
    fn init(
        init_msg: InitMsg,
        sender: std::sync::mpsc::Sender<MessageEvent<BroadcastPayload, InternalMessage>>,
    ) -> anyhow::Result<Self> {
        let (stop_sender, stop_receiver) = mpsc::channel();
        start_gossip(sender.clone(), stop_receiver);
        let responder = BroadcastResponder {
            node_id: init_msg.node_id,
            next_msg_id: 1,
            neighbours: vec![],
            messages: HashSet::new(),
            stop_sender,
            messages_to_gossip: HashMap::new(),
            gossip_sent: HashMap::new(),
        };
        anyhow::Ok(responder)
    }
    fn respond(
        &mut self,
        event: MessageEvent<BroadcastPayload, InternalMessage>,
        output: &mut impl Write,
    ) -> anyhow::Result<()> {
        match event {
            MessageEvent::EOF => {
                self.stop_sender.send(())?;
            }
            MessageEvent::Internal(InternalMessage::Gossip) => {
                for (n, msgs) in &self.messages_to_gossip {
                    let message = Message {
                        src: self.node_id.clone(),
                        dst: n.clone(),
                        body: Body {
                            id: Some(self.next_msg_id),
                            in_reply_to: None,
                            payload: BroadcastPayload::Gossip {
                                messages: msgs.clone(),
                                also_sent_to: self.neighbours.clone()
                            },
                        },
                    };
                    self.gossip_sent.insert(self.next_msg_id, (n.clone(), msgs.clone()));
                    message.send(output)?;
                    self.next_msg_id += 1;
                }
            }
            MessageEvent::External(msg) => {
                let in_reply_to = msg.body.in_reply_to;
                let mut response = msg.into_reply(Some(self.next_msg_id));
                match response.body.payload {
                    BroadcastPayload::Topology { topology } => {
                        self.neighbours = topology[&self.node_id].clone();
                        response.body.payload = BroadcastPayload::TopologyOk;
                        response.send(output)?;
                    }
                    BroadcastPayload::Broadcast { message } => {
                        if !self.messages.contains(&message) {
                            self.messages.insert(message);
                            for n in self.neighbours.clone() {
                                self.messages_to_gossip.entry(n.clone()).or_insert(HashSet::new()).insert(message);
                            }
                        }
                        response.body.payload = BroadcastPayload::BroadcastOk;
                        response.send(output)?;
                    }
                    BroadcastPayload::Read => {
                        response.body.payload = BroadcastPayload::ReadOk {
                            messages: self.messages.clone(),
                        };
                        response.send(output)?;
                    }
                    BroadcastPayload::Gossip { messages, also_sent_to} => {
                        let new_msgs = messages.iter().filter_map(|m| if self.messages.contains(m) {None} else {Some(*m)}).collect::<HashSet<usize>>();
                        if !new_msgs.is_empty() {

                            // Insert not-seen messages to be gossiped to neighbours to which these messages were not already sent by the gossip originator
                            self.messages.extend(new_msgs.clone());   
                            let nodes_to_gossip: Vec<&String> = self.neighbours.iter().filter(|n| !also_sent_to.contains(n)).collect();
                            // let nodes_to_gossip = self.neighbours.clone();
                            for n in nodes_to_gossip {
                                // Also ignore the gossip sender
                                if n.clone() != response.dst {
                                    self.messages_to_gossip.entry(n.clone()).or_insert(HashSet::new()).extend(new_msgs.clone());
                                }
                            }
                        }
                        response.body.payload = BroadcastPayload::GossipOk;
                        response.send(output)?;
                    }
                    BroadcastPayload::GossipOk => {
                        if let Some(msg_id) = in_reply_to {
                            if let Some((node_id, confirmed)) = self.gossip_sent.get(&msg_id) {
                                if let Some(messages) =
                                    self.messages_to_gossip.get(node_id)
                                {
                                    self.messages_to_gossip.insert(
                                        node_id.clone(),
                                        messages
                                            .clone()
                                            .into_iter()
                                            .filter(|m| !confirmed.contains(m))
                                            .collect(),
                                    );
                                }
                                self.gossip_sent.remove(&msg_id);
                            }
                        }
                    }
                    _ => {}
                };
                self.next_msg_id += 1;
            }
        };
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    run::<_, BroadcastResponder, _>()
}
