use std::fmt::Debug;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::{OnceCell, RwLock};
use std::time::Duration;
use mongodb::bson::doc;
use mongodb::options::FullDocumentType;
use mongodb::Collection;
use tokio::task::JoinHandle;
use crate::error;
use crate::error::MConfigError;
use futures::StreamExt;
use mongodb::change_stream::event::ChangeStreamEvent;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct MConfigEntry<V> {
    key: String,
    value: V,
}

pub struct MConfigHandler<V: Send + Sync> {
    pub(crate) key: String,
    pub(crate) collection: Collection<MConfigEntry<V>>,
    pub(crate) value: OnceCell<RwLock<Arc<V>>>,
    pub(crate) watcher: OnceCell<JoinHandle<()>>,
    pub(crate) sender: Option<tokio::sync::broadcast::Sender<Arc<V>>>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct MConfigChangeResult<V> {
    value: V,
}


impl<V: Clone + Send + Sync + Serialize + for<'de> Deserialize<'de> + Unpin + 'static + Debug> MConfigHandler<V> {
    // get a copy of current config
    pub async fn get_value(self: &Arc<MConfigHandler<V>>) -> error::Result<Arc<V>> {
        let result = self.value.get_or_try_init(|| self.init()).await;
        match result {
            Ok(lock) => {
                let guard = lock.read().await;
                Ok(guard.clone())
            }
            Err(e) => {
                Err(e)
            }
        }
    }

    // create a event receiver, can be used to run code when config is changed
    pub async fn create_new_receiver(self: &Arc<MConfigHandler<V>>) -> Option<tokio::sync::broadcast::Receiver<Arc<V>>> {
        self.sender.clone().map(|v| { v.subscribe() })
    }

    async fn init(self: &Arc<MConfigHandler<V>>) -> Result<RwLock<Arc<V>>, MConfigError> {
        self.watcher.get_or_init(|| async {
            tokio::spawn({
                let arc = self.clone();
                async move {
                    loop {
                        let is_wait = arc.watch().await;
                        if is_wait {
                            // TODO: hardcode time
                            tokio::time::sleep(Duration::from_secs(10)).await;
                        }
                    }
                }
            })
        }).await;
        let result = self.fetch_value().await;
        result.map(|v| RwLock::new(Arc::new(v)))
    }

    async fn fetch_value(self: &Arc<MConfigHandler<V>>) -> Result<V, MConfigError> {
        match self.collection.find_one(doc! {"key":self.key.clone()}).await {
            Ok(Some(task)) => {
                Ok(task.value)
            }
            Ok(None) => {
                Err(MConfigError::KeyNotExists { key: self.key.clone() })
            }
            Err(e) => {
                Err(MConfigError::MongodbError(e))
            }
        }
    }

    // keep watching current config in database, update config in memory of the one in database changed
    async fn watch(self: &Arc<MConfigHandler<V>>) -> bool {
        let pipeline = [
            doc! {
                "$match": {
                    "operationType": "update", //update|insert|delete|replace|invalidate
                    "fullDocument.key":self.key.as_str()
                }
            },
            doc! {
                "$project":{
                    // _id cannot get filtered, will get error if filtered
                    "operationType":1_i32,
                    // mongodb-rust says ns field should not get filtered
                    "ns":1_i32,
                    "fullDocument":1_i32
                }
            }
        ];
        let collection = self.collection.clone_with_type::<MConfigChangeResult<V>>();
        let mut change_stream = match collection
            .watch()
            .pipeline(pipeline)
            .full_document(FullDocumentType::UpdateLookup)
            .await
        {
            Ok(value) => { value }
            Err(_) => {
                return true;
            }
        };
        loop {
            tokio::select! {
                // _=tokio::signal::ctrl_c()=>{
                //     // stop the whole consumer
                //     return false;
                // }
                _=tokio::time::sleep(Duration::from_secs(60))=>{
                    if !change_stream.is_alive(){
                        return false;
                    }
                }
                stream_result=change_stream.next()=>{
                    if let Some(Ok(stream_event))=stream_result{
                        self.handle_stream_event(stream_event).await;
                    }else{
                        return false;
                    }
                }
            }
        }
    }
    async fn handle_stream_event(self: &Arc<MConfigHandler<V>>, stream_event: ChangeStreamEvent<MConfigChangeResult<V>>) {
        let value = match stream_event.full_document.map(|d| d.value) {
            None => {
                return;
            }
            Some(v) => {
                Arc::new(v)
            }
        };
        if let Some(sender) = &self.sender {
            let _ = sender.send(value.clone());
        }
        if let Some(lock) = self.value.get() {
            *lock.write().await = value;
        } else {
            self.value.get_or_init(|| async { RwLock::new(value) }).await;
        }
    }
}

impl<V: Send + Sync> Drop for MConfigHandler<V> {
    fn drop(&mut self) {
        if let Some(handler) = self.watcher.get() {
            handler.abort();
        }
    }
}
