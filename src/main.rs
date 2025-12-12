use anyhow::Result;
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;
mod events;
mod models;
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<()> {
    let pool = PgPool::connect("postgres://postgres:password@localhost/postgres").await?;

    let mut tx: Transaction<'_, Postgres> = pool.begin().await?;

    let user = models::User {
        id: Uuid::new_v4(),
        username: "bob".into(),
        email: "bob@company.com".into(),
    };

    sqlx::query!(
        r#"
        INSERT INTO users (id, username, email)
        VALUES ($1, $2, $3)
        "#,
        user.id,
        user.username,
        user.email,
    )
    .execute(&mut *tx)
    .await?;

    let evt = events::UserCreatedEvent {
        id: user.id,
        username: user.username,
        email: user.email,
    };

    let mut msg_headers = HashMap::new();
    msg_headers.insert("event_type".into(), "user_created".into());
    msg_headers.insert("correlation-id".into(), Uuid::new_v4().into());

    let evt_metadata = events::EventMetadata {
        topic: "user-service-events".into(),
        message_key: user.id.into(),
        message_headers: msg_headers,
    };

    let wal_message_prefix = serde_json::to_string(&evt_metadata)?;
    let wal_message_content = serde_json::to_string(&evt)?;

    sqlx::query(
        r#"
        SELECT pg_logical_emit_message(true, $1, $2)
        "#,
    )
    .bind(wal_message_prefix)
    .bind(wal_message_content)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    println!("Created user {} and outbox event", user.id);

    Ok(())
}
