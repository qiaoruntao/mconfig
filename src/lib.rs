use std::fmt::Debug;
use std::sync::{Arc};

use mongodb::{Client, Collection};
use mongodb::bson::{Bson, doc};
use mongodb::options::{ClientOptions, ResolverConfig};
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::RwLock;

use crate::error::MConfigError;

pub mod error;

pub struct MConfigClient {
    collection: Collection<Bson>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct MConfigEntry<V> {
    key: String,
    value: V,
}

impl MConfigClient {
    pub async fn create<Conn: AsRef<str>, Name: AsRef<str>>(connection_str: Conn, collection_name: Name) -> Self {
        let mut client_options = if cfg!(windows) && connection_str.as_ref().contains("+srv") {
            ClientOptions::parse_with_resolver_config(connection_str, ResolverConfig::quad9()).await.unwrap()
        } else {
            ClientOptions::parse(connection_str).await.unwrap()
        };
        let target_database = client_options.default_database.clone().unwrap();
        // Manually set an option.
        client_options.app_name = Some(collection_name.as_ref().to_string());

        // Get a handle to the deployment.
        let client = Client::with_options(client_options).unwrap();
        let database = client.database(target_database.as_str());
        let collection = database.collection(collection_name.as_ref());
        MConfigClient {
            collection,
        }
    }

    pub async fn get_handler<V: Serialize + for<'de> Deserialize<'de>, S: AsRef<str>>(self, key: S) -> Arc<MConfigHandler<V>> {
        let handler = MConfigHandler {
            key: key.as_ref().to_string(),
            collection: self.collection.clone_with_type(),
            value: Default::default(),
        };
        Arc::new(handler)
    }
}

pub struct MConfigHandler<V> {
    key: String,
    collection: Collection<MConfigEntry<V>>,
    value: RwLock<Option<Arc<V>>>,
}


impl<V: Clone + Send + Sync + Serialize + for<'de> Deserialize<'de> + Unpin> MConfigHandler<V> {
    pub async fn init_value(self: &Arc<MConfigHandler<V>>) -> error::Result<Arc<V>> {
        match self.collection.find_one(doc! {"key":self.key.clone()}, None).await {
            Ok(Some(task)) => {
                let arc = Arc::new(task.value);
                let mut guard = self.value.write().await;
                *guard = Some(arc.clone());
                Ok(arc)
            }
            Ok(None) => {
                Err(MConfigError::KeyNotExists { key: self.key.clone() })
            }
            Err(e) => {
                Err(MConfigError::MongodbError(e))
            }
        }
    }

    pub async fn get_value(self: &Arc<MConfigHandler<V>>) -> error::Result<Arc<V>> {
        let is_inited = self.is_inited().await;
        if is_inited {
            self.init_value().await
        } else {
            let guard = self.value.read().await;
            Ok(guard.as_ref().unwrap().clone())
        }
    }

    async fn is_inited(self: &Arc<MConfigHandler<V>>) -> bool {
        let guard = self.value.read().await;
        let is_inited = guard.is_none();
        is_inited
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    use super::*;

    #[tokio::test]
    async fn test_get_value() {
        let connection_str = env::var("MongoDbStr").unwrap();
        let collection_name = env::var("MongoDbCollection").unwrap();
        let client = MConfigClient::create(connection_str, collection_name).await;
        let handler = client.get_handler::<String, _>("aaa").await;
        let value = handler.get_value().await;
        assert!(value.is_ok());
        assert_eq!(value.unwrap().as_str(), "1111");
    }
}
