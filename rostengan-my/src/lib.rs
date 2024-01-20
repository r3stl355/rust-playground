use anyhow::Context;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::io::Write;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message<P> {
    pub src: String,
    #[serde(rename = "dest")]
    pub dst: String,
    pub body: Body<P>,
}

impl<P> Message<P> {
    pub fn into_reply(self, msg_id: Option<usize>) -> Self {
        Self {
            src: self.dst,
            dst: self.src,
            body: Body {
                id: msg_id,
                in_reply_to: self.body.id,
                payload: self.body.payload,
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Body<P> {
    #[serde(rename = "msg_id")]
    pub id: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<usize>,
    #[serde(flatten)]
    pub payload: P,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InitPayload {
    Init(InitMsg),
    InitOk,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct InitMsg {
    pub node_id: String,
    pub node_ids: Vec<String>,
}

pub trait Responder<P> {
    fn init(init_msg: InitMsg) -> anyhow::Result<Self>
    where
        Self: Sized;
    fn respond(&mut self, input_msg: Message<P>, output: &mut impl Write) -> anyhow::Result<()>;
}

pub fn write_out<P: Serialize>(msg: &Message<P>, output: &mut impl Write) -> anyhow::Result<()> {
    serde_json::to_writer(&mut *output, msg).context("writing out response")?;
    output.write_all(b"\n").context("writing new line")?;
    Ok(())
}



pub fn run<P, R>() -> anyhow::Result<()>
where
    P: DeserializeOwned + Send + 'static,
    R: Responder<P>,
{
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    let mut input = stdin.lines();

    let init_msg_str = &input
        .next()
        .expect("no init message")
        .context("reading init message")?;
    let init_message: Message<InitPayload> =
        serde_json::from_str(init_msg_str).context("deserializing init message")?;

    let InitPayload::Init(init_msg) = init_message.body.payload else {
        panic!("Expected init payload");
    };
    let init_response = Message {
        src: init_message.dst,
        dst: init_message.src,
        body: Body {
            id: Some(0),
            in_reply_to: init_message.body.id,
            payload: InitPayload::InitOk,
        },
    };
    write_out(&init_response, &mut stdout).context("responding to init")?;
    let mut responder: R = Responder::init(init_msg).context("create a responder")?;

    for line in input {
        let msg = line?;
        // println!("---> {}", msg);
        let message: Message<P> =
            serde_json::from_str(&msg).context("deserializing next message")?;
        responder.respond(message, &mut stdout)?;
    }

    Ok(())
}
