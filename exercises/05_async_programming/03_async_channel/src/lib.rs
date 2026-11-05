//! # Async Channel
//!
//! In this exercise, you will use `tokio::sync::mpsc` async channels to implement producer-consumer pattern.
//!
//! ## Concepts
//! - `tokio::sync::mpsc::channel` creates bounded async channels
//! - Async `send` and `recv`
//! - Channel closing mechanism (receiver returns None after all senders are dropped)

use tokio::sync::mpsc;

/// Async producer-consumer:
/// - Create a producer task that sends each element from items sequentially
/// - Create a consumer task that receives all elements and collects them into Vec for return
///
/// Hint: Set channel capacity to items.len().max(1)
pub async fn producer_consumer(items: Vec<String>) -> Vec<String> {
    let (tx, mut rx) = mpsc::channel::<String>(items.len().max(1));

    let producer = tokio::spawn(async move {
        for item in items {
            if tx.send(item).await.is_err() {
                break;
            }
        }
        // tx is dropped here, closing the channel
    });

    let consumer = tokio::spawn(async move {
        let mut result = Vec::new();
        while let Some(item) = rx.recv().await {
            result.push(item);
        }
        result
    });

    let result = consumer.await.expect("consumer task panicked");
    let _ = producer.await;
    result
}

/// Fan‑in pattern: multiple producers, one consumer.
/// Create `n_producers` producers, each sending `"producer {id}: message"`.
/// Consumer collects all messages, sorts them, and returns.
pub async fn fan_in(n_producers: usize) -> Vec<String> {
    let (tx, mut rx) = mpsc::channel::<String>(n_producers.max(1));

    let mut producer_handles = Vec::with_capacity(n_producers);
    for id in 0..n_producers {
        let tx = tx.clone();
        producer_handles.push(tokio::spawn(async move {
            let msg = format!("producer {id}: message");
            let _ = tx.send(msg).await;
            // this producer's sender is dropped here
        }));
    }

    // Drop the original sender so the channel closes once all producers finish.
    drop(tx);

    let mut result = Vec::with_capacity(n_producers);
    while let Some(msg) = rx.recv().await {
        result.push(msg);
    }

    for handle in producer_handles {
        let _ = handle.await;
    }

    result.sort();
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_producer_consumer() {
        let items = vec!["hello".into(), "async".into(), "world".into()];
        let result = producer_consumer(items.clone()).await;
        assert_eq!(result, items);
    }

    #[tokio::test]
    async fn test_producer_consumer_empty() {
        let result = producer_consumer(vec![]).await;
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_fan_in() {
        let result = fan_in(3).await;
        assert_eq!(
            result,
            vec![
                "producer 0: message",
                "producer 1: message",
                "producer 2: message",
            ]
        );
    }

    #[tokio::test]
    async fn test_fan_in_single() {
        let result = fan_in(1).await;
        assert_eq!(result, vec!["producer 0: message"]);
    }
}
