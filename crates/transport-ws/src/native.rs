use crate::{WsBackend, DEFAULT_KEEPALIVE};
use alloy_pubsub::PubSubConnect;
use alloy_transport::{utils::Spawnable, Authorization, TransportErrorKind, TransportResult};
use futures::{SinkExt, StreamExt};
use serde_json::value::RawValue;
use std::time::Duration;
use tokio::time::sleep;
use tokio_tungstenite::{
    tungstenite::{self, client::IntoClientRequest, Message},
    MaybeTlsStream, WebSocketStream,
};

type TungsteniteStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

pub use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;

/// Simple connection details for a websocket connection.
#[derive(Clone, Debug)]
pub struct WsConnect {
    /// The URL to connect to.
    url: String,
    /// The authorization header to use.
    auth: Option<Authorization>,
    /// Additional headers to send with the handshake request.
    ///
    /// All values are marked as sensitive, so they are redacted in `Debug` output.
    headers: http::HeaderMap,
    /// The websocket config.
    config: Option<WebSocketConfig>,
    /// Max number of retries before failing and exiting the connection.
    /// Default is 10.
    max_retries: u32,
    /// The base interval between retries.
    ///
    /// Reconnect retries use capped exponential backoff from this base interval.
    /// Default is 3 seconds.
    retry_interval: Duration,
    /// The interval between keepalive pings.
    /// Default is 10 seconds.
    keepalive_interval: Duration,
}

impl WsConnect {
    /// Creates a new websocket connection configuration.
    ///
    /// If the URL contains credentials (e.g. `wss://user:pass@host`), they are
    /// automatically extracted and set as the [`Authorization`] header.
    pub fn new<S: Into<String>>(url: S) -> Self {
        let url = url.into();
        let auth =
            url::Url::parse(&url).ok().and_then(|parsed| Authorization::extract_from_url(&parsed));
        Self {
            url,
            auth,
            headers: http::HeaderMap::new(),
            config: None,
            max_retries: 10,
            retry_interval: Duration::from_secs(3),
            keepalive_interval: Duration::from_secs(DEFAULT_KEEPALIVE),
        }
    }

    /// Sets the authorization header.
    pub fn with_auth(mut self, auth: Authorization) -> Self {
        self.auth = Some(auth);
        self
    }

    /// Sets the optional authorization header.
    ///
    /// This replaces the current [`Authorization`].
    pub fn with_auth_opt(mut self, auth: Option<Authorization>) -> Self {
        self.auth = auth;
        self
    }

    /// Sets a custom HTTP header to send with the websocket handshake request.
    ///
    /// This replaces any header previously set with the same name. The value is marked as
    /// sensitive, so it is redacted in `Debug` output.
    ///
    /// Headers set here are applied before the [`Authorization`] header, so an explicit
    /// [`with_auth`](Self::with_auth) (or credentials in the URL) take precedence over a custom
    /// `authorization` header.
    ///
    /// This is only available on native targets: browsers do not allow setting arbitrary
    /// headers on the websocket handshake, so the wasm `WsConnect` does not support it.
    ///
    /// # Examples
    ///
    /// ```
    /// use alloy_transport_ws::WsConnect;
    /// use http::{HeaderName, HeaderValue};
    ///
    /// let ws = WsConnect::new("wss://example.com")
    ///     .with_header(HeaderName::from_static("x-token"), HeaderValue::from_static("secret"));
    /// assert_eq!(ws.headers()["x-token"], "secret");
    /// ```
    pub fn with_header(mut self, name: http::HeaderName, mut value: http::HeaderValue) -> Self {
        value.set_sensitive(true);
        self.headers.insert(name, value);
        self
    }

    /// Extends the custom HTTP headers sent with the websocket handshake request.
    ///
    /// For each header name present in `headers`, any values previously set for that name are
    /// replaced. All values are marked as sensitive, so they are redacted in `Debug` output.
    ///
    /// See [`with_header`](Self::with_header) for how these interact with [`Authorization`] and
    /// for platform support.
    pub fn with_headers(mut self, mut headers: http::HeaderMap) -> Self {
        for value in headers.values_mut() {
            value.set_sensitive(true);
        }
        self.headers.extend(headers);
        self
    }

    /// Sets the websocket config.
    pub const fn with_config(mut self, config: WebSocketConfig) -> Self {
        self.config = Some(config);
        self
    }

    /// Get the URL string of the connection.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Get the authorization header.
    pub const fn auth(&self) -> Option<&Authorization> {
        self.auth.as_ref()
    }

    /// Get the custom HTTP headers sent with the websocket handshake request.
    pub const fn headers(&self) -> &http::HeaderMap {
        &self.headers
    }

    /// Get the websocket config.
    pub const fn config(&self) -> Option<&WebSocketConfig> {
        self.config.as_ref()
    }

    /// Sets the max number of retries before failing and exiting the connection.
    /// Default is 10.
    pub const fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Sets the base interval between retries.
    ///
    /// Reconnect retries use capped exponential backoff from this base interval.
    /// Default is 3 seconds.
    pub const fn with_retry_interval(mut self, retry_interval: Duration) -> Self {
        self.retry_interval = retry_interval;
        self
    }

    /// Sets the keepalive ping interval.
    ///
    /// A ping is sent if no other messages have been sent within this interval.
    /// If the server does not respond with a pong before the next ping is due,
    /// the connection is considered dead and will be closed.
    ///
    /// Default is 10 seconds.
    pub const fn with_keepalive_interval(mut self, keepalive_interval: Duration) -> Self {
        self.keepalive_interval = keepalive_interval;
        self
    }
}

impl IntoClientRequest for WsConnect {
    fn into_client_request(self) -> tungstenite::Result<tungstenite::handshake::client::Request> {
        let mut request: http::Request<()> = self.url.into_client_request()?;
        // Custom headers go first so that an explicit `auth` overrides a custom `authorization`.
        request.headers_mut().extend(self.headers);
        if let Some(auth) = self.auth {
            let mut auth_value = http::HeaderValue::from_str(&auth.to_string())?;
            auth_value.set_sensitive(true);

            request.headers_mut().insert(http::header::AUTHORIZATION, auth_value);
        }

        request.into_client_request()
    }
}

impl PubSubConnect for WsConnect {
    fn is_local(&self) -> bool {
        alloy_transport::utils::guess_local_url(&self.url)
    }

    async fn connect(&self) -> TransportResult<alloy_pubsub::ConnectionHandle> {
        #[cfg(any(feature = "aws-lc-rs", feature = "ring"))]
        install_default_crypto_provider();

        let request = self.clone().into_client_request();
        let req = request.map_err(TransportErrorKind::custom)?;
        let (socket, _) = tokio_tungstenite::connect_async_with_config(req, self.config, false)
            .await
            .map_err(TransportErrorKind::custom)?;

        let (handle, interface) = alloy_pubsub::ConnectionHandle::new();
        let backend = WsBackend { socket, interface, keepalive_interval: self.keepalive_interval };

        backend.spawn();

        Ok(handle.with_max_retries(self.max_retries).with_retry_interval(self.retry_interval))
    }
}

/// Install a default rustls crypto provider if none is set.
///
/// Required since rustls 0.23+ no longer auto-installs one.
#[cfg(any(feature = "aws-lc-rs", feature = "ring"))]
fn install_default_crypto_provider() {
    if rustls::crypto::CryptoProvider::get_default().is_some() {
        return;
    }
    #[cfg(feature = "aws-lc-rs")]
    let provider = rustls::crypto::aws_lc_rs::default_provider();
    #[cfg(all(feature = "ring", not(feature = "aws-lc-rs")))]
    let provider = rustls::crypto::ring::default_provider();
    // install_default returns Err if a concurrent caller raced us past the get_default check;
    // either provider is valid.
    let _ = rustls::crypto::CryptoProvider::install_default(provider);
}

impl WsBackend<TungsteniteStream> {
    /// Handle a message from the server.
    #[expect(clippy::result_unit_err)]
    pub fn handle(&mut self, msg: Message) -> Result<(), ()> {
        match msg {
            Message::Text(text) => self.handle_text(&text),
            Message::Close(frame) => {
                if frame.is_some() {
                    error!(?frame, "Received close frame with data");
                } else {
                    error!("WS server has gone away");
                }
                Err(())
            }
            Message::Binary(_) => {
                error!("Received binary message, expected text");
                Err(())
            }
            Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => Ok(()),
        }
    }

    /// Send a message to the server.
    pub async fn send(&mut self, msg: Box<RawValue>) -> Result<(), tungstenite::Error> {
        self.socket.send(Message::Text(msg.get().to_owned().into())).await
    }

    /// Spawn a new backend task.
    pub fn spawn(mut self) {
        let fut = async move {
            let mut errored = false;
            let mut expecting_pong = false;
            let keepalive = sleep(self.keepalive_interval);
            tokio::pin!(keepalive);
            loop {
                // We bias the loop as follows
                // 1. New dispatch to server.
                // 2. Keepalive.
                // 3. Response or notification from server.
                // This ensures that keepalive is sent only if no other messages
                // have been sent in the keepalive interval. And prioritizes new
                // dispatches over responses from the server. This will fail if
                // the client saturates the task with dispatches, but that's
                // probably not a big deal.
                tokio::select! {
                    biased;
                    // we've received a new dispatch, so we send it via
                    // websocket. We handle new work before processing any
                    // responses from the server.
                    inst = self.interface.recv_from_frontend() => {
                        match inst {
                            Some(msg) => {
                                // Reset the keepalive timer.
                                keepalive.set(sleep(self.keepalive_interval));
                                if let Err(err) = self.send(msg).await {
                                    error!(%err, "WS connection error");
                                    errored = true;
                                    break
                                }
                            },
                            // dispatcher has gone away, or shutdown was received
                            None => {
                                break
                            },
                        }
                    },
                    // Send a ping to the server, if no other messages have been
                    // sent within the keepalive interval.
                    _ = &mut keepalive => {
                        // Still expecting a pong from the previous ping,
                        // meaning connection is errored.
                        if expecting_pong {
                            error!("WS server missed a pong");
                            errored = true;
                            break
                        }
                        // Reset the keepalive timer.
                        keepalive.set(sleep(self.keepalive_interval));
                        if let Err(err) = self.socket.send(Message::Ping(Default::default())).await {
                            error!(%err, "WS connection error");
                            errored = true;
                            break
                        }
                        // Expecting to receive a pong before the next
                        // keepalive timer resolves.
                        expecting_pong = true;
                    }
                    resp = self.socket.next() => {
                        match resp {
                            Some(Ok(item)) => {
                                if item.is_pong() {
                                    expecting_pong = false;
                                }
                                errored = self.handle(item).is_err();
                                if errored { break }
                            },
                            Some(Err(err)) => {
                                error!(%err, "WS connection error");
                                errored = true;
                                break
                            }
                            None => {
                                error!("WS server has gone away");
                                errored = true;
                                break
                            },
                        }
                    }
                }
            }
            if errored {
                self.interface.close_with_error();
            }
        };
        fut.spawn_task()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_from_url() {
        for (url, auth) in [
            ("wss://user:pass@example.com/path", Some(Authorization::basic("user", "pass"))),
            ("ws://user@example.com", Some(Authorization::basic("user", ""))),
            ("wss://example.com/rpc", None),
            ("ws://localhost:8545", None),
        ] {
            let ws = WsConnect::new(url);
            assert_eq!(ws.url(), url);
            assert_eq!(ws.auth(), auth.as_ref(), "{url}");
        }
    }

    #[test]
    fn explicit_auth_overrides_url_auth() {
        let ws =
            WsConnect::new("wss://user:pass@example.com").with_auth(Authorization::bearer("tok"));
        assert_eq!(ws.auth(), Some(&Authorization::bearer("tok")));
    }

    /// Accepts a single websocket connection and returns the handshake request headers.
    async fn capture_handshake_headers(
        ws: WsConnect,
        listener: tokio::net::TcpListener,
    ) -> http::HeaderMap {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let callback =
                |req: &tungstenite::handshake::server::Request,
                 resp: tungstenite::handshake::server::Response| {
                    tx.send(req.headers().clone()).unwrap();
                    Ok(resp)
                };
            tokio_tungstenite::accept_hdr_async(stream, callback).await.unwrap()
        });
        let _handle = ws.connect().await.unwrap();
        let headers = rx.await.unwrap();
        server.abort();
        headers
    }

    async fn local_listener() -> (tokio::net::TcpListener, String) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        (listener, url)
    }

    #[tokio::test]
    async fn custom_header_sent_on_handshake() {
        let (listener, url) = local_listener().await;
        let ws =
            WsConnect::new(url).with_header("x-token".parse().unwrap(), "secret".parse().unwrap());
        let headers = capture_handshake_headers(ws, listener).await;
        assert_eq!(headers.get("x-token").unwrap(), "secret");
    }

    #[tokio::test]
    async fn explicit_auth_overrides_custom_authorization_header() {
        let (listener, url) = local_listener().await;
        let ws = WsConnect::new(url)
            .with_header(http::header::AUTHORIZATION, "Bearer custom".parse().unwrap())
            .with_auth(Authorization::bearer("explicit"));
        let headers = capture_handshake_headers(ws, listener).await;
        let values: Vec<_> = headers.get_all(http::header::AUTHORIZATION).iter().collect();
        assert_eq!(values, ["Bearer explicit"]);
    }

    #[test]
    fn header_values_redacted_in_debug() {
        let mut map = http::HeaderMap::new();
        map.insert("x-other", "other-secret".parse().unwrap());
        let ws = WsConnect::new("wss://example.com")
            .with_header("x-token".parse().unwrap(), "super-secret".parse().unwrap())
            .with_headers(map);
        assert!(ws.headers().values().all(http::HeaderValue::is_sensitive));
        let debug = format!("{ws:?}");
        assert!(debug.contains("x-token"), "{debug}");
        assert!(!debug.contains("super-secret"), "{debug}");
        assert!(!debug.contains("other-secret"), "{debug}");
    }

    #[test]
    fn with_headers_replaces_same_name() {
        let mut map = http::HeaderMap::new();
        map.insert("x-token", "new".parse().unwrap());
        let ws = WsConnect::new("wss://example.com")
            .with_header("x-token".parse().unwrap(), "old".parse().unwrap())
            .with_header("x-keep".parse().unwrap(), "keep".parse().unwrap())
            .with_headers(map);
        assert_eq!(ws.headers().get_all("x-token").iter().collect::<Vec<_>>(), ["new"]);
        assert_eq!(ws.headers()["x-keep"], "keep");
    }
}
