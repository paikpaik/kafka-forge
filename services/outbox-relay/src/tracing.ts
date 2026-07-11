import { NodeSDK } from "@opentelemetry/sdk-node";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";

const sdk = new NodeSDK({
  serviceName: "outbox-relay",
  traceExporter: new OTLPTraceExporter({ url: "http://localhost:4318/v1/traces" }),
});

sdk.start();
