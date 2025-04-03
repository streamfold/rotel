use std::error::Error;
use std::time::Duration;
use tokio::task::JoinSet;
use tokio::time::{Instant, timeout_at};
use tracing::error;

pub async fn wait_for_any_task(
    tasks: &mut JoinSet<Result<(), Box<dyn Error + Send + Sync>>>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let r = tasks.join_next().await;

    match r {
        None => Ok(()), // should not happen
        Some(res) => res?,
    }
}

pub async fn wait_for_tasks_with_timeout(
    tasks: &mut JoinSet<Result<(), Box<dyn Error + Send + Sync>>>,
    timeout: Duration,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    wait_for_tasks_with_deadline(tasks, Instant::now() + timeout).await
}

pub async fn wait_for_tasks_with_deadline(
    tasks: &mut JoinSet<Result<(), Box<dyn Error + Send + Sync>>>,
    stop_at: Instant,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut result = Ok(());
    loop {
        match timeout_at(stop_at, tasks.join_next()).await {
            Err(_) => {
                result = Err("timed out waiting for tasks to complete".into());
                break;
            }
            Ok(None) => break,
            Ok(Some(v)) => {
                match v {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => result = Err(e),
                    e => {
                        error!("Failed to join with task: {:?}", e)
                    } // Ignore?
                }
            }
        }
    }

    result
}
