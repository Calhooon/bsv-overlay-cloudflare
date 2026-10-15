//! Worker transport for the engine's SHIP and SLAP peer discovery.

use async_trait::async_trait;
use overlay_engine::ship_lookup::ShipLookupClient;
use overlay_engine::types::{LookupQuestion, OutputListItem};

/// Bounds the complete lookup, including reading its response body.
/// Matches `MAX_TRACKER_WAIT_TIME` in ts-stack@edf6e03
/// packages/sdk/src/overlay-tools/LookupResolver.ts:248.
pub(crate) const SHIP_DISCOVERY_LOOKUP_TIMEOUT_MS: u64 =
    bsv_rs::overlay::types::MAX_SHIP_QUERY_TIMEOUT_MS;

/// The Worker's [`ShipLookupClient`]: a bounded `POST /lookup` over fetch.
pub struct WorkerShipLookupClient;

#[async_trait(?Send)]
impl ShipLookupClient for WorkerShipLookupClient {
    async fn lookup(
        &self,
        host: &str,
        question: &LookupQuestion,
    ) -> Result<Vec<OutputListItem>, String> {
        lookup_with_deadline(
            fetch_lookup(host, question),
            crate::broadcaster::sleep_ms(SHIP_DISCOVERY_LOOKUP_TIMEOUT_MS),
        )
        .await
    }
}

async fn lookup_with_deadline(
    lookup: impl std::future::Future<Output = Result<Vec<OutputListItem>, String>>,
    deadline: impl std::future::Future<Output = ()>,
) -> Result<Vec<OutputListItem>, String> {
    overlay_engine::gasp::race_or_deadline(lookup, deadline)
        .await
        .ok_or_else(|| format!("lookup exceeded {SHIP_DISCOVERY_LOOKUP_TIMEOUT_MS} ms"))?
}

async fn fetch_lookup(
    host: &str,
    question: &LookupQuestion,
) -> Result<Vec<OutputListItem>, String> {
    let url = format!("{}/lookup", host.trim_end_matches('/'));
    let body = serde_json::to_string(question).map_err(|e| e.to_string())?;
    let headers = worker::Headers::new();
    headers
        .set("Content-Type", "application/json")
        .map_err(|e| e.to_string())?;
    let mut init = worker::RequestInit::new();
    init.with_method(worker::Method::Post)
        .with_headers(headers)
        .with_body(Some(body.into()));
    let request = worker::Request::new_with_init(&url, &init).map_err(|e| e.to_string())?;
    let mut response = worker::Fetch::Request(request)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let status = response.status_code();
    if !(200..300).contains(&status) {
        return Err(format!("peer lookup returned HTTP {status}"));
    }
    let text = response.text().await.map_err(|e| e.to_string())?;
    parse_output_list(&text)
}

fn parse_output_list(body: &str) -> Result<Vec<OutputListItem>, String> {
    #[derive(serde::Deserialize)]
    struct Answer {
        #[serde(rename = "type")]
        kind: String,
        #[serde(default)]
        outputs: Vec<OutputListItem>,
    }
    let answer: Answer = serde_json::from_str(body).map_err(|e| format!("lookup JSON: {e}"))?;
    Ok(if answer.kind == "output-list" {
        answer.outputs
    } else {
        Vec::new()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_output_list_preserves_beef_and_output_index() {
        let outputs = parse_output_list(
            r#"{"type":"output-list","outputs":[
            {"beef":[1,2,3],"outputIndex":4}, {"beef":[5,6],"outputIndex":1}
        ]}"#,
        )
        .unwrap();
        assert_eq!(outputs.len(), 2);
        assert_eq!(outputs[0].beef, vec![1, 2, 3]);
        assert_eq!(outputs[0].output_index, 4);
        assert_eq!(outputs[1].output_index, 1);
    }

    #[test]
    fn lookup_non_output_answers_are_empty_and_bad_json_is_a_fault() {
        assert!(
            parse_output_list(r#"{"type":"free-text-reply","result":"none"}"#)
                .unwrap()
                .is_empty()
        );
        assert!(parse_output_list(r#"{"type":"output-list","outputs":[]}"#)
            .unwrap()
            .is_empty());
        assert!(parse_output_list("invalid JSON").is_err());
    }

    #[tokio::test]
    async fn lookup_deadline_drops_a_hung_call() {
        let result = lookup_with_deadline(std::future::pending(), std::future::ready(())).await;
        assert!(result.unwrap_err().contains("5000 ms"));
        let result = lookup_with_deadline(async { Ok(Vec::new()) }, std::future::pending()).await;
        assert!(result.unwrap().is_empty());
    }
}
