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
    },
}

struct BroadcastResponder {
    node_id: String,
    msg_id: usize,
    neighbours: Vec<String>,
    messages: HashSet<usize>,
    stop_sender: mpsc::Sender<()>,
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
        std::thread::sleep(Duration::from_micros(500));

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
            msg_id: 1,
            neighbours: vec![],
            messages: HashSet::new(),
            stop_sender: stop_sender,
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
                // Construct a set of broadcasts
                for n in self.neighbours.clone() {
                    let msg = Message {
                        src: self.node_id.clone(),
                        dst: n,
                        body: Body {
                            id: Some(self.msg_id),
                            in_reply_to: None,
                            payload: BroadcastPayload::Gossip {
                                messages: self.messages.clone(),
                            },
                        },
                    };
                    msg.send(output)?;
                    self.msg_id += 1;
                }
            }
            MessageEvent::External(msg) => {
                let mut response = msg.into_reply(Some(self.msg_id));
                match response.body.payload {
                    BroadcastPayload::Topology { topology } => {
                        self.neighbours = topology[&self.node_id].clone();
                        response.body.payload = BroadcastPayload::TopologyOk;
                        response.send(output)?;
                    }
                    BroadcastPayload::Broadcast { message } => {
                        if !self.messages.contains(&message) {
                            self.messages.insert(message);

                            response.body.payload = BroadcastPayload::BroadcastOk;
                            response.send(output)?;
                        }
                    }
                    BroadcastPayload::Read => {
                        response.body.payload = BroadcastPayload::ReadOk {
                            messages: self.messages.clone(),
                        };
                        response.send(output)?;
                    }
                    BroadcastPayload::Gossip { messages } => {
                        self.messages.extend(messages);
                    }
                    _ => {}
                };
                self.msg_id += 1;
            }
        };
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    run::<_, BroadcastResponder, _>()
}
