use std::collections::HashMap;

use anyhow::Context;
use solana_pubkey::Pubkey;
use tokio::task;

use crate::{
    config::{Config, Endpoint},
    proto::geyser::{
        SubscribeDeshredRequest, SubscribeRequestFilterDeshredTransactions, SubscribeRequestPing,
        SubscribeUpdateDeshred, subscribe_update_deshred::UpdateOneof,
    },
};

use super::{
    GeyserProvider, ProviderContext,
    yellowstone::{ProviderError, TransactionUpdate, connect_endpoint, receive_transactions},
};

pub struct DeshredProvider;

impl GeyserProvider for DeshredProvider {
    fn process(
        &self,
        endpoint: Endpoint,
        config: Config,
        context: ProviderContext,
    ) -> task::JoinHandle<Result<(), ProviderError>> {
        task::spawn(async move {
            let shutdown_tx = context.shutdown_tx.clone();
            let result = process_deshred_endpoint(endpoint, config, context).await;
            if result.is_err() {
                let _ = shutdown_tx.send(());
            }
            result
        })
    }
}

async fn process_deshred_endpoint(
    endpoint: Endpoint,
    config: Config,
    context: ProviderContext,
) -> Result<(), ProviderError> {
    config.account.parse::<Pubkey>()?;
    let mut client = connect_endpoint(&endpoint).await?;
    // Deshred is pre-execution and has no commitment or execution-status filter.
    let request = SubscribeDeshredRequest {
        deshred_transactions: HashMap::from([(
            "account".to_string(),
            SubscribeRequestFilterDeshredTransactions {
                account_include: vec![config.account],
                ..Default::default()
            },
        )]),
        ..Default::default()
    };
    let (subscribe_tx, stream) = client
        .subscribe_deshred_with_request(Some(request))
        .await
        .with_context(|| format!("SubscribeDeshred failed for endpoint {}", endpoint.name))?;
    receive_transactions(
        endpoint,
        context,
        subscribe_tx,
        stream,
        SubscribeDeshredRequest {
            ping: Some(SubscribeRequestPing { id: 1 }),
            ..Default::default()
        },
    )
    .await
}

impl From<SubscribeUpdateDeshred> for TransactionUpdate {
    fn from(update: SubscribeUpdateDeshred) -> Self {
        match update.update_oneof {
            // The server filters both static keys and loaded ALT addresses.
            Some(UpdateOneof::DeshredTransaction(tx)) => {
                Self::Transaction(tx.transaction.map(|tx| tx.signature))
            }
            Some(UpdateOneof::Ping(_)) => Self::Ping,
            _ => Self::Other,
        }
    }
}
