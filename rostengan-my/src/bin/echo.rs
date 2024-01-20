use anyhow::Context;
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
    fn init(_init_msg: InitMsg) -> anyhow::Result<Self> {
        anyhow::Ok(EchoResponder { msg_id: 1 })
    }
    fn respond(
        &mut self,
        input_msg: Message<EchoPayload>,
        output: &mut impl Write,
    ) -> anyhow::Result<()> {

        // `input_msg` will be moveed here (`into_reply` has `self`) and cannot be accessed after
        let mut response = input_msg.into_reply(Some(self.msg_id));
        match response.body.payload {
            EchoPayload::Echo { echo } => {
                response.body.payload = EchoPayload::EchoOk { echo };
                write_out(&response, output).context("responding to init")?;
            }
            _ => {}
        };
        self.msg_id += 1;
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    run::<_, EchoResponder>()
}
