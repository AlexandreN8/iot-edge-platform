import os
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

OTEL_EXPORTER_ENDPOINT = os.environ.get("OTEL_EXPORTER_ENDPOINT", "localhost:4317")

_propagator = TraceContextTextMapPropagator()
_tracer_provider_configured = False


def get_tracer(service_name):
    """
    Configures the global tracer provider once (guarded, same pattern as
    logging_setup.get_logger avoiding duplicate handlers) and returns a
    tracer for this service.
    """
    global _tracer_provider_configured
    if not _tracer_provider_configured:
        provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
        exporter = OTLPSpanExporter(endpoint=OTEL_EXPORTER_ENDPOINT, insecure=True)
        provider.add_span_processor(BatchSpanProcessor(exporter))
        trace.set_tracer_provider(provider)
        _tracer_provider_configured = True
    return trace.get_tracer(service_name)


def inject_trace_context(carrier=None):
    """
    Returns a dict carrying the current span's W3C traceparent - meant to
    be embedded as a field in an outgoing message payload, since MQTT
    3.1.1 (unlike Kafka) has no native per-message header mechanism to
    carry this out of band.
    """
    carrier = carrier if carrier is not None else {}
    _propagator.inject(carrier)
    return carrier


def extract_trace_context(carrier):
    """ Returns a Context built from a traceparent found in an incoming payload's carrier dict. """
    return _propagator.extract(carrier)