import http from "node:http";
import { metricsRegistry } from "kafka-forge";

export function startMetricsServer(port: number): void {
  http
    .createServer(async (req, res) => {
      if (req.url === "/metrics") {
        res.setHeader("Content-Type", metricsRegistry.contentType);
        res.end(await metricsRegistry.metrics());
      } else {
        res.statusCode = 404;
        res.end();
      }
    })
    .listen(port, () => console.log(`/metrics 노출: http://localhost:${port}/metrics`));
}
