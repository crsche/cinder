#[macro_use]
extern crate tracing;

use std::{
    // net::{SocketAddr, TcpListener},
    borrow::Borrow,
    net::SocketAddr,
    ptr::addr_eq,
};

use anyhow::Result;
use axum::{
    extract::Host,
    handler::{self, HandlerWithoutStateExt},
    http::{uri::Scheme, StatusCode, Uri},
    response::{IntoResponse, Redirect},
    routing::get,
    BoxError, Router,
};
use axum_server::tls_rustls::RustlsConfig;
use listenfd::ListenFd;
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;
use tower_http::{compression::CompressionLayer, services::ServeDir};

#[derive(Debug, Clone, Copy)]
struct Ports {
    http: u16,
    https: u16,
}

const PORTS: Ports = Ports {
    http: 8080,
    https: 8080,
};

const CERT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/certs/cert.pem");
const KEY: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/certs/key.pem");

const STATIC: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/static");

async fn hello() -> &'static str {
    "Hello, World!"
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let tls_config = RustlsConfig::from_pem_file(CERT, KEY).await?;
    debug!(?tls_config, "loaded TLS configuration");

    let compression = CompressionLayer::new()
        .br(true)
        .deflate(true)
        .gzip(true)
        .zstd(true);
    debug!("compression enabled");

    let static_ = ServeDir::new(STATIC);
    debug!(?static_, "serving static files");

    let app = Router::new()
        .nest_service("/static", static_)
        .route("/", get(hello))
        .layer(TraceLayer::new_for_http())
        .layer(compression)
        .fallback(handler_404);

    let mut listenfd = ListenFd::from_env();
    let listener = match listenfd.take_tcp_listener(0)? {
        Some(listener) => {
            listener.set_nonblocking(true)?;
            std::net::TcpListener::from(listener)
            // TcpListener::from_std(listener)?
        }
        None => {
            let addr = SocketAddr::from(([127, 0, 0, 1], PORTS.https));
            std::net::TcpListener::bind(addr)?
            // TcpListener::bind(addr).await?
        }
    };
    tokio::spawn(http_to_https(PORTS));

    axum_server::from_tcp_rustls(listener, tls_config)
        .serve(app.into_make_service())
        .await?;
    Ok(())
}

async fn handler_404() -> impl IntoResponse {
    (StatusCode::NOT_FOUND, "404 nothing to see here")
}

async fn http_to_https(ports: Ports) -> Result<()> {
    fn make_https(host: String, uri: Uri, ports: Ports) -> Result<Uri, BoxError> {
        let mut parts = uri.into_parts();
        parts.scheme = Some(Scheme::HTTPS);
        if parts.path_and_query.is_none() {
            parts.path_and_query = Some("/".parse()?);
        }
        let https_host = host.replace(&ports.http.to_string(), &ports.https.to_string());
        parts.authority = Some(https_host.parse()?);
        Ok(Uri::from_parts(parts)?)
    }

    debug!("redirecting HTTP traffic to HTTPS");

    let redir = move |Host(host), uri: Uri| async move {
        match make_https(host, uri, ports) {
            Ok(uri) => Ok(Redirect::permanent(&uri.to_string())),
            Err(e) => {
                warn!(%e, "failed to redirect to HTTPS");
                Err(StatusCode::BAD_REQUEST)
            }
        }
    };

    let addr = SocketAddr::from(([127, 0, 0, 1], ports.http));
    let listener = TcpListener::bind(addr).await?;
    debug!(?listener, "listening for HTTP traffic");
    axum::serve(listener, redir.into_make_service()).await?;
    Ok(())
}
