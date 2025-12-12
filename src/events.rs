use serde::Serialize;
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Serialize, Debug)]
pub struct EventMetadata {
    pub topic: String,
    pub message_key: String,
    pub message_headers: HashMap<String, String>,
}

#[derive(Serialize, Debug)]
pub struct UserCreatedEvent {
    pub id: Uuid,
    pub username: String,
    pub email: String,
}
