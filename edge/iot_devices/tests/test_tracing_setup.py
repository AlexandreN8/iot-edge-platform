from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry import trace
from tracing_setup import inject_trace_context, extract_trace_context


def test_inject_trace_context_includes_traceparent():
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = provider.get_tracer("test")

    with tracer.start_as_current_span("test-span"):
        carrier = inject_trace_context()

    assert "traceparent" in carrier
    assert carrier["traceparent"].startswith("00-")


def test_extract_trace_context_roundtrip_preserves_trace_id():
    provider = TracerProvider()
    tracer = provider.get_tracer("test")

    with tracer.start_as_current_span("test-span") as span:
        carrier = inject_trace_context()
        original_trace_id = span.get_span_context().trace_id

    ctx = extract_trace_context(carrier)
    extracted_span_context = trace.get_current_span(ctx).get_span_context()
    assert extracted_span_context.trace_id == original_trace_id