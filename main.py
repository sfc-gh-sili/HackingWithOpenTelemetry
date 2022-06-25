import typing
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    SpanExporter,
    SpanExportResult
)
from opentelemetry.exporter.otlp.proto.http.trace_exporter.encoder import _ProtobufEncoder
from opentelemetry.sdk.trace import ReadableSpan

# XP code start
class UDFSpanExporter(SpanExporter):
    def __init__(self):
        self._shutdown = False

    def export(
        self, spans: typing.Sequence[ReadableSpan]
    ) -> "SpanExportResult":
        """Exports a batch of telemetry data. Overrides SpanExporter.export()
        Args:g
            spans: The list of `opentelemetry.trace.Span` objects to be exported
        Returns:
            The result of the export
        """
        if self._shutdown:
            return SpanExportResult.FAILURE

        # serialized to ExportTraceServiceRequest
        # https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/trace/v1/trace.proto
        serialized_data = _ProtobufEncoder.serialize(spans)
        print(serialized_data)
        return SpanExportResult.SUCCESS

    def shutdown(self) -> None:
        """Shuts down the exporter. Overrides SpanExporter.shutdown()
        Called when the SDK is shut down.
        """
        if self._shutdown:
            raise Exception("Python UDF tracing has already been shutdown.")

        self._shutdown = True
        # _snowflake.write_event_traces()


def initialize():
    trace.set_tracer_provider(TracerProvider())
    trace.get_tracer_provider().add_span_processor(BatchSpanProcessor(UDFSpanExporter()))
# XP code end


# User code start
from opentelemetry import trace
tracer = trace.get_tracer("udf")
def do_work():
    with tracer.start_as_current_span("span-name") as span:
        print("doing some work...")
# User code end


if __name__ == '__main__':
    initialize()
    do_work()