use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::{io::Write, collections::HashMap};
use std::collections::HashSet;

use rostengan_my::*;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum BroadcastPayload {
    Topology {topology: HashMap<String, Vec<String>>},
    TopologyOk,
    Broadcast {message: usize},
    BroadcastOk,
    Read,
    ReadOk {messages: HashSet<usize>}
}

struct BroadcastResponder {
    node_id: String,
    msg_id: usize,
    topology: HashMap<String, Vec<String>>,
    messages: HashSet<usize>
}
// {:id 9871870, :src "n5", :dest "n4", :body {:msg_id 0, :in_reply_to nil, :type "broadcast", :message 0}}
// {:type "broadcast", :message 0, :msg_id 1}
//{:id 10655934, :src "n2", :dest "n1", :body {:msg_id 1035670, :in_reply_to 0, :type "broadcast_ok"}}

impl BroadcastResponder {

    fn broadcast(&mut self, message: usize, output: &mut impl Write) -> anyhow::Result<()> {
        for target in self.topology.get(&self.node_id).unwrap_or(&Vec::new()) {
            // Increment the message ID before sending because this will be called after `BroadcastOk` which
            // does not increment the ID
            self.msg_id += 1;
            let msg = Message {
                src: self.node_id.clone(),
                dst: (*target).clone(),
                body: Body {
                    id: Some(self.msg_id),
                    in_reply_to: None,
                    payload: BroadcastPayload::Broadcast { message: message },
                },
            };
            write_out(&msg, output).context("broadcasting to a neighbour")?;
        }
        Ok(())
    }
}

impl Responder<BroadcastPayload> for BroadcastResponder {
    fn init(init_msg: InitMsg) -> anyhow::Result<Self> {
        anyhow::Ok(BroadcastResponder {node_id: init_msg.node_id,  msg_id: 1, topology: HashMap::new(), messages: HashSet::new()})
    }
    fn respond(
        &mut self,
        input_msg: Message<BroadcastPayload>,
        output: &mut impl Write,
    ) -> anyhow::Result<()> {
        let mut response = input_msg.into_reply(Some(self.msg_id));
        match response.body.payload {
            BroadcastPayload::Topology { topology } => {
                self.topology = topology;
                response.body.payload = BroadcastPayload::TopologyOk;
                write_out(&response, output).context("responding to `topology` message")?;
            },
            BroadcastPayload::Broadcast { message } => {
                if !self.messages.contains(&message) {
                    self.messages.insert(message);
                    // self.messages.extend(vec![message]);
                    
                    response.body.payload = BroadcastPayload::BroadcastOk;
                    write_out(&response, output).context("responding to `broadcast` message")?;

                    // Need to broadcast to other nodes here
                    self.broadcast(message, output)?;
                }
            },
            BroadcastPayload::Read => {
                response.body.payload = BroadcastPayload::ReadOk { messages: self.messages.clone()};
                write_out(&response, output).context("responding to `read` message")?;
            },
            _ => {}
        };
        self.msg_id += 1;
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    run::<_, BroadcastResponder>()
}
