use serde::{Deserialize, Serialize};
use std::io::Write;

use rostengan_my::*;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum EchoPayload {
    Echo { echo: String },
    EchoOk { echo: String },
}

struct EchoResponder {
    msg_id: usize,
}

impl Responder<EchoPayload> for EchoResponder {
    fn init(
        _init_msg: InitMsg,
        _responder: std::sync::mpsc::Sender<MessageEvent<EchoPayload, ()>>,
    ) -> anyhow::Result<Self> {
        anyhow::Ok(EchoResponder { msg_id: 1 })
    }
    fn respond(
        &mut self,
        event: MessageEvent<EchoPayload, ()>,
        output: &mut impl Write,
    ) -> anyhow::Result<()> {
        let MessageEvent::External(msg) = event else {
            panic!("Expected Echo payload message")
        };

        // `msg` will be moveed here (`into_reply` has `self`) and cannot be accessed after
        let mut response = msg.into_reply(Some(self.msg_id));
        match response.body.payload {
            EchoPayload::Echo { echo } => {
                response.body.payload = EchoPayload::EchoOk { echo };
                response.send(output)?;
            }
            _ => {}
        };
        self.msg_id += 1;
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    run::<_, EchoResponder, _>()
}
