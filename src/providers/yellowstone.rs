use std::{collections::HashMap, error::Error, sync::atomic::Ordering};

use futures_util::{Sink, Stream, sink::SinkExt, stream::StreamExt};
use solana_pubkey::Pubkey;
use tokio::task;
use tonic::{Status, transport::ClientTlsConfig};
use tracing::{Level, info, warn};
use yellowstone_grpc_client::GeyserGrpcClient;

use crate::proto::geyser::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterTransactions, SubscribeRequestPing,
    SubscribeUpdate, subscribe_update::UpdateOneof,
};
use crate::{
    config::{Config, Endpoint},
    utils::{TransactionData, get_current_timestamp, open_log_file, write_log_entry},
};

use super::{
    GeyserProvider, ProviderContext,
    common::{TransactionAccumulator, build_signature_envelope, enqueue_signature},
};

pub(super) type ProviderError = Box<dyn Error + Send + Sync>;

pub struct YellowstoneProvider;

impl GeyserProvider for YellowstoneProvider {
    fn process(
        &self,
        endpoint: Endpoint,
        config: Config,
        context: ProviderContext,
    ) -> task::JoinHandle<Result<(), ProviderError>> {
        task::spawn(async move {
            let shutdown_tx = context.shutdown_tx.clone();
            let result = process_yellowstone_endpoint(endpoint, config, context).await;
            // A failed subscription cannot contribute to the shared signature target.
            if result.is_err() {
                let _ = shutdown_tx.send(());
            }
            result
        })
    }
}

async fn process_yellowstone_endpoint(
    endpoint: Endpoint,
    config: Config,
    context: ProviderContext,
) -> Result<(), ProviderError> {
    config.account.parse::<Pubkey>()?;
    let mut client = connect_endpoint(&endpoint).await?;
    let commitment: CommitmentLevel = config.commitment.into();
    let request = SubscribeRequest {
        transactions: HashMap::from([(
            "account".to_string(),
            SubscribeRequestFilterTransactions {
                account_include: vec![config.account],
                ..Default::default()
            },
        )]),
        commitment: Some(commitment as i32),
        ..Default::default()
    };
    let (subscribe_tx, stream) = client.subscribe_with_request(Some(request)).await?;
    receive_transactions(
        endpoint,
        context,
        subscribe_tx,
        stream,
        SubscribeRequest {
            ping: Some(SubscribeRequestPing { id: 1 }),
            ..Default::default()
        },
    )
    .await
}

pub(super) async fn connect_endpoint(
    endpoint: &Endpoint,
) -> Result<GeyserGrpcClient, ProviderError> {
    info!(endpoint = %endpoint.name, url = %endpoint.url, "Connecting");
    let token = endpoint
        .x_token
        .clone()
        .filter(|token| !token.trim().is_empty());
    let mut builder = GeyserGrpcClient::build_from_shared(endpoint.url.clone())?.x_token(token)?;
    if endpoint.url.starts_with("https://") {
        builder = builder.tls_config(ClientTlsConfig::new().with_native_roots())?;
    }
    let client = builder.connect().await?;
    info!(endpoint = %endpoint.name, "Connected");
    Ok(client)
}

pub(super) enum TransactionUpdate {
    Transaction(Option<Vec<u8>>),
    Ping,
    Other,
}

impl From<SubscribeUpdate> for TransactionUpdate {
    fn from(update: SubscribeUpdate) -> Self {
        match update.update_oneof {
            // Trust the server's account_include filter, which also matches ALT accounts.
            Some(UpdateOneof::Transaction(tx)) => {
                Self::Transaction(tx.transaction.map(|tx| tx.signature))
            }
            Some(UpdateOneof::Ping(_)) => Self::Ping,
            _ => Self::Other,
        }
    }
}

pub(super) async fn receive_transactions<S, T, U, R>(
    endpoint: Endpoint,
    context: ProviderContext,
    mut subscribe_tx: T,
    mut stream: S,
    ping_request: R,
) -> Result<(), ProviderError>
where
    S: Stream<Item = Result<U, Status>> + Unpin,
    T: Sink<R> + Unpin,
    T::Error: Error + Send + Sync + 'static,
    U: Into<TransactionUpdate>,
    R: Clone,
{
    let ProviderContext {
        shutdown_tx,
        mut shutdown_rx,
        start_wallclock_secs,
        start_instant,
        comparator,
        signature_tx,
        shared_counter,
        shared_shutdown,
        target_transactions,
        total_producers,
        progress,
    } = context;
    let endpoint_name = endpoint.name;
    let mut log_file = if tracing::enabled!(Level::TRACE) {
        Some(open_log_file(&endpoint_name)?)
    } else {
        None
    };
    let mut accumulator = TransactionAccumulator::new();
    let mut transaction_count = 0usize;

    let result = loop {
        tokio::select! { biased;
            _ = shutdown_rx.recv() => {
                info!(endpoint = %endpoint_name, "Received stop signal");
                break Ok(());
            }
            message = stream.next() => {
                // Capture local receive time before inspecting, encoding, logging or recording.
                // All providers in this run share the same monotonic start_instant.
                let elapsed = start_instant.elapsed();
                let wallclock = get_current_timestamp();
                match message {
                    Some(Ok(msg)) => match msg.into() {
                        TransactionUpdate::Transaction(signature) => {
                            let Some(signature) = signature.filter(|signature| signature.len() == 64) else {
                                warn!(endpoint = %endpoint_name, "Missing or invalid transaction signature");
                                continue;
                            };
                            let signature = bs58::encode(signature).into_string();
                            if let Some(file) = log_file.as_mut() {
                                write_log_entry(file, wallclock, &endpoint_name, &signature)?;
                            }
                            let tx_data = TransactionData {
                                wallclock_secs: wallclock,
                                elapsed_since_start: elapsed,
                                start_wallclock_secs,
                            };
                            let updated = accumulator.record(signature.clone(), tx_data.clone());
                            if updated
                                && let Some(envelope) = build_signature_envelope(
                                    &comparator, &endpoint_name, &signature, tx_data, total_producers,
                                ) {
                                    if let Some(target) = target_transactions {
                                        let shared = shared_counter.fetch_add(1, Ordering::AcqRel) + 1;
                                        if let Some(tracker) = progress.as_ref() {
                                            tracker.record(shared);
                                        }
                                        if shared >= target
                                            && !shared_shutdown.swap(true, Ordering::AcqRel)
                                        {
                                            info!(endpoint = %endpoint_name, target, "Reached shared signature target; broadcasting shutdown");
                                            let _ = shutdown_tx.send(());
                                        }
                                    }
                                    if let Some(sender) = signature_tx.as_ref() {
                                        enqueue_signature(sender, &endpoint_name, &signature, envelope);
                                    }
                                }
                            transaction_count += 1;
                        }
                        TransactionUpdate::Ping => {
                            if let Err(err) = subscribe_tx.send(ping_request.clone()).await {
                                break Err(Box::new(err) as ProviderError);
                            }
                        }
                        TransactionUpdate::Other => {}
                    },
                    Some(Err(err)) => break Err(Box::new(err) as ProviderError),
                    None => {
                        info!(endpoint = %endpoint_name, "Stream closed by server");
                        // Stop peers too: no more common signatures can be completed.
                        let _ = shutdown_tx.send(());
                        break Ok(());
                    }
                }
            }
        }
    };

    let unique_signatures = accumulator.len();
    comparator.add_batch(&endpoint_name, accumulator.into_inner());
    info!(
        endpoint = %endpoint_name,
        total_transactions = transaction_count,
        unique_signatures,
        "Stream closed after dispatching transactions"
    );
    result
}
