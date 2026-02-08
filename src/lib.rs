use std::sync::Arc;

use mongodb::{Client, Collection};
use mongodb::bson::Bson;
use mongodb::options::ClientOptions;
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
        let mut client_options = ClientOptions::parse(connection_str.as_ref()).await.unwrap();
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

    pub async fn get_handler<V: Serialize + for<'de> Deserialize<'de> + Send + Sync, S: AsRef<str>>(&self, key: S) -> Arc<MConfigHandler<V>> {
        let handler = MConfigHandler {
            key: key.as_ref().to_string(),
            collection: self.collection.clone_with_type(),
            value: Default::default(),
            watcher: Default::default(),
            sender: None,
        };
        Arc::new(handler)
    }

    pub async fn get_handler_with_channel<V: Serialize + for<'de> Deserialize<'de> + Send + Sync, S: AsRef<str>>(&self, key: S, receiver_cnt: usize) -> Arc<MConfigHandler<V>> {
        let (sender, _) = tokio::sync::broadcast::channel(receiver_cnt);
        let handler = MConfigHandler {
            key: key.as_ref().to_string(),
            collection: self.collection.clone_with_type(),
            value: Default::default(),
            watcher: Default::default(),
            sender: Some(sender),
        };
        Arc::new(handler)
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::time::Duration;
    use mongodb::bson::doc;

    use super::*;

    #[tokio::test]
    async fn test_get_value() {
        let connection_str = env::var("MongoDbStr").unwrap();
        let collection_name = env::var("MongoDbCollection").unwrap();
        let client = MConfigClient::create(connection_str.as_str(), collection_name.as_str()).await;
        // use a different mongodb client to update values, otherwise watch event will be filtered
        let client2 = MConfigClient::create(connection_str.as_str(), collection_name.as_str()).await;

        let collection = client2.collection.clone_with_type();
        // remove existing key
        collection.delete_one(doc! {"key":"aaa"}, None).await.expect("failed to remove existing key");
        // init value
        let first_value = "1111";
        let second_value = "11111";
        collection.insert_one(doc! {"key":"aaa", "value":first_value}, None).await.expect("failed to insert key");

        let handler = client.get_handler_with_channel::<String, _>("aaa", 10).await;
        let (first_try, second_try) = tokio::join!(handler.get_value(),handler.get_value());

        assert!(first_try.is_ok());
        assert_eq!(first_try.as_ref().unwrap().as_str(), first_value);
        assert!(second_try.is_ok());
        assert_eq!(second_try.as_ref().unwrap().as_str(), first_value);
        let join_handler = tokio::spawn({
            let handler = handler.clone();
            async move {
                let mut receiver = handler.create_new_receiver().await.unwrap();
                let arc = receiver.recv().await;
                assert!(arc.is_ok());
                assert_eq!(arc.as_ref().unwrap().as_str(), second_value);
            }
        });
        collection.update_one(doc! {"key":"aaa"}, doc! {"$set":{"value":second_value}}, None).await.expect("failed to update value");
        // let wait_seconds = 5;
        // tokio::time::sleep(Duration::from_secs(wait_seconds)).await;
        join_handler.await.expect("async wait failed");
        let third_try = handler.get_value().await;
        assert!(third_try.is_ok());
        // new config is updated now
        assert_eq!(third_try.unwrap().as_str(), second_value);
        // previous config will not get updated
        assert_eq!(first_try.as_ref().unwrap().as_str(), first_value);
        assert_eq!(second_try.as_ref().unwrap().as_str(), first_value);
        // wait for async task display
        tokio::time::sleep(Duration::from_secs(1)).await;
        // clean up
        collection.delete_one(doc! {"key":"aaa"}, None).await.expect("failed to clean up");
    }
}
