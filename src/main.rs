use tracing::{info, info_span, Instrument};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Registry;
use opentelemetry::trace::{TracerProvider, TraceContextExt};

#[tokio::main]
async fn main() {
    // Setup tracing with OpenTelemetry
    let tracer = opentelemetry_stdout::SpanExporter::default();
    let provider = opentelemetry_sdk::trace::TracerProvider::builder()
        .with_simple_exporter(tracer)
        .build();
    let tracer = provider.tracer("repro");

    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
    let subscriber = Registry::default().with(telemetry);
    tracing::subscriber::set_global_default(subscriber).unwrap();

    println!("\n========================================");
    println!("🔴 BEFORE FIX: tokio::spawn WITHOUT .instrument()");
    println!("========================================\n");
    simulate_aws_chunked_stream_broken().await;

    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    println!("\n========================================");
    println!("✅ AFTER FIX: tokio::spawn WITH .instrument()");
    println!("========================================\n");
    simulate_aws_chunked_stream_fixed().await;

    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    opentelemetry::global::shutdown_tracer_provider();
}

async fn simulate_aws_chunked_stream_broken() {
    let request_span = info_span!("http_request", request_id = "req-001");
    let _guard = request_span.enter();
    info!("📨 Processing HTTP request");
    print_current_trace_info("http_request handler");

    let middleware_span = info_span!("s3_auth_middleware");
    let _m_guard = middleware_span.enter();
    info!("🔐 In s3_auth middleware");
    print_current_trace_info("s3_auth middleware");

    let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(10);
    
    tokio::spawn(async move {
        info!("🔴 Worker task started (NO CONTEXT)");
        print_current_trace_info("worker task (BROKEN)");
        for i in 1..=3 {
            let chunk_span = info_span!("process_chunk", chunk_num = i);
            let _chunk_guard = chunk_span.enter();
            info!("Processing chunk #{}", i);
            print_current_trace_info(&format!("chunk {} processing", i));
            let _ = tx.send(format!("chunk-{}", i)).await;
        }
    });

    while let Some(chunk) = rx.recv().await {
        info!("📦 Received: {}", chunk);
    }
}

async fn simulate_aws_chunked_stream_fixed() {
    let request_span = info_span!("http_request", request_id = "req-002");
    let _guard = request_span.enter();
    info!("📨 Processing HTTP request");
    print_current_trace_info("http_request handler");

    let middleware_span = info_span!("s3_auth_middleware");
    let _m_guard = middleware_span.enter();
    info!("🔐 In s3_auth middleware");
    print_current_trace_info("s3_auth middleware");

    let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(10);
    let span = tracing::Span::current();
    
    tokio::spawn(
        async move {
            info!("✅ Worker task started (WITH CONTEXT)");
            print_current_trace_info("worker task (FIXED)");
            for i in 1..=3 {
                let chunk_span = info_span!("process_chunk", chunk_num = i);
                let _chunk_guard = chunk_span.enter();
                info!("Processing chunk #{}", i);
                print_current_trace_info(&format!("chunk {} processing", i));
                let _ = tx.send(format!("chunk-{}", i)).await;
            }
        }
        .instrument(span)
    );

    while let Some(chunk) = rx.recv().await {
        info!("📦 Received: {}", chunk);
    }
}

fn print_current_trace_info(location: &str) {
    let current_span = tracing::Span::current();
    if current_span.is_none() {
        println!("  ⚠️  [{}] NO SPAN CONTEXT!", location);
        return;
    }

    current_span.with_subscriber(|(id, subscriber)| {
        println!("  📍 [{}] Span ID: {:?}", location, id);
        use tracing_subscriber::registry::LookupSpan;
        if let Some(reg) = subscriber.downcast_ref::<Registry>() {
            if let Some(span_ref) = reg.span(id) {
                let extensions = span_ref.extensions();
                if let Some(otel_data) = extensions.get::<tracing_opentelemetry::OtelData>() {
                    let ctx = otel_data.parent_cx.clone();
                    let span_ref = ctx.span();
                    let span_context = span_ref.span_context();
                    println!("     🔗 Trace ID: {}", span_context.trace_id());
                    println!("     🆔 Span ID:  {}", span_context.span_id());
                    return;
                }
            }
        }
        println!("  ⚠️  [{}] Span exists but no OpenTelemetry context", location);
    });
}
