use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;
use tokio::sync::broadcast::{Receiver, Sender};
use tower::BoxError;
use tracing::log::warn;
use crate::bounded_channel::{bounded, BoundedReceiver, BoundedSender};

const FLUSH_CHAN_SIZE:usize = 20;

#[derive(Debug, Clone)]
pub struct FlushRequest {
    id: u64,
}

#[derive(Debug, Clone)]
pub struct FlushResponse {
    id: u64,
}

pub struct FlushBroadcast {
    inner: Arc<Mutex<Inner>>,

    req_tx: Sender<FlushRequest>,
    resp_rx: BoundedReceiver<FlushResponse>,
    resp_tx: BoundedSender<FlushResponse>,
}

struct Inner {
    listeners: usize,

    _req_rx: Receiver<FlushRequest>, // must keep open, but we subscribe from tx to create listeners
}

impl Inner {
    fn new(req_rx: Receiver<FlushRequest>) -> Self {
        Self{
            listeners: 0,
            _req_rx: req_rx,
        }
    }

    pub fn add_subscriber(&mut self) {
        self.listeners += 1;
    }
}

impl FlushBroadcast {
    pub fn new() -> Self {
        let (req_tx, req_rx) = broadcast::channel(FLUSH_CHAN_SIZE);
        let (resp_tx, resp_rx) = bounded(FLUSH_CHAN_SIZE);

        Self {
            inner: Arc::new(Mutex::new(Inner::new(req_rx))),
            req_tx,
            resp_tx,
            resp_rx,
        }
    }

    pub fn into_parts(self) -> (FlushPublisher, FlushSubscriber) {
        let publisher = FlushPublisher{
            inner: self.inner.clone(),
            next_req_id: 1,
            req_tx: self.req_tx.clone(),
            resp_rx: self.resp_rx,
        };
        let subscriber = FlushSubscriber {
            inner: self.inner.clone(),
            req_tx: self.req_tx.clone(),
            resp_tx: self.resp_tx,
        };

        (publisher, subscriber)
    }
}

pub struct FlushPublisher {
    inner: Arc<Mutex<Inner>>,
    next_req_id: u64,
    req_tx: Sender<FlushRequest>,
    resp_rx: BoundedReceiver<FlushResponse>,
}

impl FlushPublisher {
    pub async fn broadcast(&mut self) -> Result<(), BoxError> {
        let curr_listeners = self.inner.lock().unwrap().listeners;
        let req_id = self.next_req_id;
        self.next_req_id += 1;
        let req = FlushRequest{id: req_id};

        if let Err(e) = self.req_tx.send(req) {
            return Err(format!("Unable to send broadcast message: {}", e).into());
        }

        let mut acked = 0u64;
        loop {
            if acked == curr_listeners as u64 {
                break
            }
            match self.resp_rx.next().await {
                None => {
                    return Err("unexpected close received on flush response channel".into());
                }
                Some(resp) => {
                    if resp.id != req_id {
                        warn!("invalid response id received, expected {}, got {}", req_id, resp.id);
                        continue
                    }
                    acked += 1;
                }
            }
        }
        Ok(())
    }

}

pub struct FlushSubscriber {
    inner: Arc<Mutex<Inner>>,
    req_tx: Sender<FlushRequest>,
    resp_tx: BoundedSender<FlushResponse>,
}

impl FlushSubscriber {
    pub fn subscribe(&mut self) -> FlushListener {
        self.inner.lock().unwrap().add_subscriber();

        FlushListener {
            rx: self.req_tx.subscribe(),
            tx: self.resp_tx.clone(),
        }

    }
}

pub struct FlushListener {
    rx: Receiver<FlushRequest>,
    tx: BoundedSender<FlushResponse>
}

impl FlushListener {
    pub async fn next(&mut self) -> Option<FlushRequest> {
        match self.rx.recv().await {
            Ok(item) => Some(item),
            Err(_e) => None, // disconnected
        }
    }

    pub async fn ack(&mut self, req: FlushRequest) -> Result<(), BoxError>{
        let resp = FlushResponse{id: req.id};
        self.tx.send(resp).await.map_err(|e| format!("failed to ack: {}", e).into())
    }
}

