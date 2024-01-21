use anyhow::Context;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::io::{BufRead, Write};
use std::sync::mpsc::{self};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message<P> {
    pub src: String,
    #[serde(rename = "dest")]
    pub dst: String,
    pub body: Body<P>,
}

impl<P> Message<P>
where
    P: Serialize,
{
    pub fn into_reply(self, msg_id: Option<usize>) -> Self {
        Self {
            src: self.dst,
            dst: self.src,
            body: Body {
                id: msg_id,
                in_reply_to: self.body.id,
                payload: self.body.payload,
            },
        }
    }
    pub fn send(self, output: &mut impl Write) -> anyhow::Result<()> {
        serde_json::to_writer(&mut *output, &self).context("writing out response")?;
        output.write_all(b"\n").context("writing new line")?;
        Ok(())
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

pub enum MessageEvent<P, I> {
    External(Message<P>),
    Internal(I),
    EOF,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct InitMsg {
    pub node_id: String,
    pub node_ids: Vec<String>,
}

pub trait Responder<P, I = ()> {
    fn init(init_msg: InitMsg, sender: mpsc::Sender<MessageEvent<P, I>>) -> anyhow::Result<Self>
    where
        Self: Sized;
    fn respond(&mut self, event: MessageEvent<P, I>, output: &mut impl Write)
        -> anyhow::Result<()>;
}

pub fn run<P, R, I>() -> anyhow::Result<()>
where
    P: DeserializeOwned + Send + 'static,
    R: Responder<P, I>,
    I: Send + 'static,
{
    // Using `lock` will require `BufRead` to work
    let stdin = std::io::stdin().lock();
    let mut stdin = stdin.lines();
    let mut stdout = std::io::stdout().lock();

    let init_msg_str = &stdin
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
    init_response.send(&mut stdout)?;

    let (sender, receiver) = mpsc::channel::<MessageEvent<P, I>>();
    let mut responder: R =
        Responder::init(init_msg, sender.clone()).context("create a responder")?;

    drop(stdin);
    let input_runnner = std::thread::spawn(move || {
        let stdin = std::io::stdin().lock();
        let stdin = stdin.lines();
        for line in stdin {
            let message: Message<P> = serde_json::from_str(&line.unwrap())
                .context("deserializing next message")
                .unwrap();
            sender.send(MessageEvent::External(message)).unwrap();
        }
        sender.send(MessageEvent::EOF).unwrap();
        Ok::<(), anyhow::Error>(())
    });

    for msg in receiver {
        responder.respond(msg, &mut stdout)?;
    }
    input_runnner
        .join()
        .expect("process to terminate gracefully")?;

    Ok(())
}
