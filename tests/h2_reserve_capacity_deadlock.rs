//! Regression test for Issue #4003: H2 reserve_capacity(1) cross-stream deadlock.
//!
//! Bug: `PipeToSendStream` previously called `reserve_capacity(1)` at the top
//! of every loop iteration, BEFORE polling the body. When a stream had just
//! buffered data via `send_data()`, h2 internally tracks `buffered_send_data`.
//! On the next iteration, `reserve_capacity(1)` computes
//! `effective = 1 + buffered_send_data`, which claims the LAST byte of the
//! connection's available flow control capacity. Other streams are then
//! starved — they cannot obtain any capacity to send data.
//!
//! The deadlock is made permanent by using a server that never calls
//! `release_capacity()` (so no WINDOW_UPDATE is sent), combined with a
//! streaming body that sends `window_size - 1` bytes and then stalls.
//!
//! Fix: Poll the body FIRST, then reserve exact capacity for the actual data.
//! When the body returns `Pending`, no capacity is held — other streams can
//! proceed.

#![cfg(feature = "http2")]

use std::convert::Infallible;
use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use http::Request;
use hyper::body::Frame;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

mod support;
use support::TokioIo;

/// H2 spec default window size.
const WINDOW_SIZE: u32 = 65_535;

// ---------------------------------------------------------------------------
// No-release H2 server
// ---------------------------------------------------------------------------

/// Start a raw h2 server that reads request body data but NEVER calls
/// `release_capacity()`. Without `release_capacity`, no WINDOW_UPDATE frames
/// are sent, so the client's connection send window is never replenished.
async fn start_no_release_server() -> (SocketAddr, oneshot::Sender<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel();

    tokio::spawn(async move {
        loop {
            tokio::select! {
                result = listener.accept() => {
                    let (stream, _) = result.unwrap();
                    tokio::spawn(async move {
                        let mut builder = h2::server::Builder::default();
                        builder.initial_window_size(WINDOW_SIZE);
                        builder.initial_connection_window_size(WINDOW_SIZE);

                        let mut conn = match builder.handshake(stream).await {
                            Ok(c) => c,
                            Err(_) => return,
                        };

                        while let Some(result) = conn.accept().await {
                            let (req, mut respond) = match result {
                                Ok(v) => v,
                                Err(_) => break,
                            };

                            tokio::spawn(async move {
                                let mut body = req.into_body();

                                // Read ALL request body data — but NEVER call
                                // release_capacity(). This prevents WINDOW_UPDATE
                                // from being sent to the client.
                                while let Some(chunk) = body.data().await {
                                    match chunk {
                                        Ok(data) => {
                                            // Intentionally NOT calling:
                                            // body.flow_control().release_capacity(data.len());
                                            let _ = data.len();
                                        }
                                        Err(_) => return,
                                    }
                                }

                                // Send response after reading the full body.
                                let resp = http::Response::builder()
                                    .status(200)
                                    .body(())
                                    .unwrap();
                                let mut send = match respond.send_response(resp, false) {
                                    Ok(s) => s,
                                    Err(_) => return,
                                };
                                let _ = send.send_data(Bytes::from("ok"), true);
                            });
                        }
                    });
                }
                _ = &mut shutdown_rx => break,
            }
        }
    });

    (addr, shutdown_tx)
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

/// Minimal reproduction of Issue #4003 using standard stream-based bodies
/// and specific payload sizes. No custom Body impl, no coordination channels.
///
/// - Stream A: `stream::iter([65534 bytes]).chain(stream::pending())`
///   Yields one 65534-byte frame then returns Pending forever.
/// - Stream B: `stream::iter([1 byte])`
///   Yields one 1-byte frame then ends.
///
/// The payloads are chosen so Stream A fills all but 1 byte of the
/// connection window. The buggy `reserve_capacity(1)` then claims
/// that last byte, starving Stream B.
#[tokio::test]
async fn h2_reserve_capacity_deadlock() {
    use futures_util::{stream, StreamExt as _};
    use http_body_util::{combinators::BoxBody, StreamBody};

    type BoxedBody = BoxBody<Bytes, Infallible>;

    let (addr, _shutdown) = start_no_release_server().await;

    let stream = TcpStream::connect(addr).await.unwrap();
    let io = TokioIo::new(stream);

    let (mut sender, conn) = hyper::client::conn::http2::Builder::new(support::TokioExecutor)
        .initial_stream_window_size(WINDOW_SIZE)
        .initial_connection_window_size(WINDOW_SIZE)
        .timer(support::TokioTimer)
        .handshake::<_, BoxedBody>(io)
        .await
        .unwrap();

    tokio::spawn(conn);

    // Payload A: exactly WINDOW_SIZE - 1 = 65534 bytes, then stall forever.
    // stream::iter yields the single frame, stream::pending returns Pending
    // on every subsequent poll — the body stalls with no coordination needed.
    let body_a: BoxedBody = BoxBody::new(StreamBody::new(
        stream::iter([Ok::<_, Infallible>(Frame::data(Bytes::from(
            vec![0xAA; (WINDOW_SIZE - 1) as usize],
        )))])
        .chain(stream::pending()),
    ));

    let req_a = Request::post("http://localhost/a").body(body_a).unwrap();
    let _resp_a = sender.send_request(req_a);

    // Yield so the conn task processes Stream A's payload and enters the
    // stalled state where reserve_capacity(1) has claimed the last byte.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Payload B: exactly 1 byte. stream::iter yields the frame, then the
    // stream ends — no coordination needed.
    let body_b: BoxedBody = BoxBody::new(StreamBody::new(stream::iter([Ok::<_, Infallible>(
        Frame::data(Bytes::from_static(b"B")),
    )])));

    let req_b = Request::post("http://localhost/b").body(body_b).unwrap();
    let resp_b_fut = sender.send_request(req_b);

    // With the fix: Stream B completes within the timeout.
    // Before the fix: Stream B hangs forever (deadlock).
    let stream_b_result = tokio::time::timeout(Duration::from_secs(5), resp_b_fut).await;

    assert!(
        stream_b_result.is_ok(),
        "Stream B should complete (fix resolves the deadlock)"
    );
    let resp_b = stream_b_result.unwrap().expect("Stream B response");
    assert_eq!(resp_b.status(), 200);
}
