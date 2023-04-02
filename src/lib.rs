use std::sync::Arc;

use mongodb::{Client, Collection};
use mongodb::bson::Bson;
use mongodb::options::{ClientOptions, ResolverConfig};
use serde::Deserialize;
use serde::Serialize;

use handler::MConfigHandler;

pub mod error;
pub mod handler;

pub struct MConfigClient {
    collection: Collection<Bson>,
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
            watcher: Default::default(),
        };
        Arc::new(handler)
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn test_get_value() {
        let connection_str = env::var("MongoDbStr").unwrap();
        let collection_name = env::var("MongoDbCollection").unwrap();
        let client = MConfigClient::create(connection_str, collection_name).await;
        let handler = client.get_handler::<String, _>("aaa").await;
        let (first_try, second_try) = tokio::join!(handler.get_value(),handler.get_value());
        assert!(first_try.is_ok());
        assert_eq!(first_try.unwrap().as_str(), "1111");
        assert!(second_try.is_ok());
        assert_eq!(second_try.unwrap().as_str(), "1111");
        tokio::time::sleep(Duration::from_secs(20)).await;
        let third_try = handler.get_value().await;
        assert!(third_try.is_ok());
        assert_eq!(third_try.unwrap().as_str(), "11111");
    }
}
