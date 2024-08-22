from typing import Any

from prometheus_client import Gauge, start_http_server

prom_metrics: dict[str, Any] = {}


def set_prom_metric(metric_name: str, value: Any):
    if metric_name in prom_metrics:
        prom_metrics[metric_name].set(value)


def initialise_prom_metrics_and_start_server(metric_defs: list[tuple], port: int):

    for metric_def in metric_defs:
        prom_metrics[metric_def[0]] = Gauge(metric_def[0], metric_def[1])

    start_http_server(port)
