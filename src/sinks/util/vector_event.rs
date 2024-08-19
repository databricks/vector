// Structs used for our vector event logs
use std::collections::HashMap;
use std::env;
use once_cell::sync::OnceCell;
use serde_json;

use crate::event::Event;
use crate::event::LogEvent;

// Struct for vector send events (sending, uploaded)
#[derive(Clone, Debug)]
pub struct VectorSendEventMetadata {
    pub bytes: usize,
    pub events_len: usize,
    pub blob: String,
    pub container: String,
    pub count_map: HashMap<String, usize>,
}

pub static COUNT_MAP_KEYS: OnceCell<Vec<String>> = OnceCell::new();

pub fn get_count_map_keys() -> &'static Vec<String> {
    // Initialize the static variable once, or return the value if it's already initialized/computed
    COUNT_MAP_KEYS.get_or_init(|| {
            let vec_string: String = env::var("COUNT_MAP_KEY").unwrap_or_else(|_| "".to_string());
            let keys: Vec<String> = serde_json::from_str(&vec_string).unwrap_or(Vec::new());
            keys
        }
    )
}

impl VectorSendEventMetadata {
    pub fn emit_upload_event(&self) {
        info!(
            message = "Uploaded events.",
            bytes = self.bytes,
            events_len = self.events_len,
            blob = self.blob,
            container = self.container,
            // VECTOR_UPLOADED_MESSAGES_EVENT
            vector_event_type = 4
        );
    }

    pub fn emit_sending_event(&self) {
        info!(
            message = "Sending events.",
            bytes = self.bytes,
            events_len = self.events_len,
            blob = self.blob,
            container = self.container,
            // VECTOR_SENDING_MESSAGES_EVENT
            vector_event_type = 3
        );
        info!(
            message = "Test granularity change.",
            map = serde_json::to_string(&self.count_map).unwrap(),
            vector_event_type = 1,
        )
    }
}

// Build key for count map like "key1,key2" as POC
pub fn build_key(event: &LogEvent) -> String {
    let mut key_vals: Vec<String> = Vec::new();
    for key_part in get_count_map_keys() {
        if let Ok(value) = event.parse_path_and_get_value(&key_part) {
            if let Some(val) = value {
               // Remove extra quotes from string
               key_vals.push(val.to_string().replace("\"", ""));
            }
        }
    }
    key_vals.join(",")
}

pub fn generate_count_map(events: &Vec<Event>) -> HashMap<String, usize> {
    let mut count_map = HashMap::new();
    for event in events {
        // Check if it's a log event (see enum defined in lib/vector-core/src/event/mod.rs)
        if let Event::Log(log_event) = event {
            count_map.entry(build_key(log_event)).and_modify(|x| *x += 1).or_insert(1);
        }
    }
    count_map
}
