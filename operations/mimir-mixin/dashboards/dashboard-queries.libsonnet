local utils = import 'mixin-utils/utils.libsonnet';

{
  // This object contains common queries used in the Mimir dashboards.
  // These queries are NOT intended to be configurable or overriddeable via jsonnet,
  // but they're defined in a common place just to share them between different dashboards.
  queries:: {
    // Define the supported replacement variables in a single place. Most of them are frequently used.
    local variables = {
      gatewayMatcher: $.jobMatcher($._config.job_names.gateway),
      distributorMatcher: $.jobMatcher($._config.job_names.distributor),
      queryFrontendMatcher: $.jobMatcher($._config.job_names.query_frontend),
      rulerMatcher: $.jobMatcher($._config.job_names.ruler),
      alertmanagerMatcher: $.jobMatcher($._config.job_names.alertmanager),
      namespaceMatcher: $.namespaceMatcher(),
      writeHTTPRoutesRegex: $.queries.write_http_routes_regex,
      writeGRPCRoutesRegex: $.queries.write_grpc_routes_regex,
      readHTTPRoutesRegex: $.queries.read_http_routes_regex,
      perClusterLabel: $._config.per_cluster_label,
      recordingRulePrefix: $.recordingRulePrefix($.jobSelector('any')),  // The job name does not matter here.
      groupPrefixJobs: $._config.group_prefix_jobs,
    },

    write_http_routes_regex: 'api_(v1|prom)_push|otlp_v1_metrics',
    write_grpc_routes_regex: '/distributor.Distributor/Push|/httpgrpc.*',
    read_http_routes_regex: '(prometheus|api_prom)_api_v1_.+',
    query_http_routes_regex: '(prometheus|api_prom)_api_v1_query(_range)?',

    gateway: {
      local p = self,
      writeRequestsPerSecondMetric: 'cortex_request_duration_seconds',
      writeRequestsPerSecondSelector: '%(gatewayMatcher)s, route=~"%(writeHTTPRoutesRegex)s"' % variables,
      // deprecated, will be removed
      writeRequestsPerSecond: '%s{%s}' % [p.writeRequestsPerSecondMetric, p.writeRequestsPerSecondSelector],
      readRequestsPerSecondMetric: 'cortex_request_duration_seconds',
      readRequestsPerSecondSelector: '%(gatewayMatcher)s, route=~"%(readHTTPRoutesRegex)s"' % variables,
      // deprecated, will be removed
      readRequestsPerSecond: '%s{%s}' % [p.readRequestsPerSecondMetric, p.readRequestsPerSecondSelector],

      // Write failures rate as percentage of total requests.
      writeFailuresRate:: {
        local template = |||
          (
              # gRPC errors are not tracked as 5xx but "error".
              sum(%(countFailQuery)s)
              or
              # Handle the case no failure has been tracked yet.
              vector(0)
          )
          /
          sum(%(countQuery)s)
        |||,
        classic: template % {
          countFailQuery: utils.nativeClassicHistogramCountRate(p.writeRequestsPerSecondMetric, p.writeRequestsPerSecondSelector + ',status_code=~"5.*|error"').classic,
          countQuery: utils.nativeClassicHistogramCountRate(p.writeRequestsPerSecondMetric, p.writeRequestsPerSecondSelector).classic,
        },
        native: template % {
          countFailQuery: utils.nativeHistogramCountRate(p.writeRequestsPerSecondMetric, p.writeRequestsPerSecondSelector + ',status_code=~"5.*|error"').native,
          countQuery: utils.nativeHistogramCountRate(p.writeRequestsPerSecondMetric, p.writeRequestsPerSecondSelector).native,
        },
      },

      // Read failures rate as percentage of total requests.
      readFailuresRate:: {
        local template = |||
          (
              # gRPC errors are not tracked as 5xx but "error".
              sum(%(countFailQuery)s)
              or
              # Handle the case no failure has been tracked yet.
              vector(0)
          )
          /
          sum(%(countQuery)s)
        |||,
        classic: template % {
          countFailQuery: utils.nativeClassicHistogramCountRate(p.readRequestsPerSecondMetric, p.readRequestsPerSecondSelector + ',status_code=~"5.*|error"').classic,
          countQuery: utils.nativeClassicHistogramCountRate(p.readRequestsPerSecondMetric, p.readRequestsPerSecondSelector).classic,
        },
        native: template % {
          countFailQuery: utils.nativeClassicHistogramCountRate(p.readRequestsPerSecondMetric, p.readRequestsPerSecondSelector + ',status_code=~"5.*|error"').native,
          countQuery: utils.nativeClassicHistogramCountRate(p.readRequestsPerSecondMetric, p.readRequestsPerSecondSelector).native,
        },
      },
    },

    distributor: {
      local p = self,
      writeRequestsPerSecondMetric: 'cortex_request_duration_seconds',
      writeRequestsPerSecondSelector: '%(distributorMatcher)s, route=~"%(writeGRPCRoutesRegex)s|%(writeHTTPRoutesRegex)s"' % variables,
      // deprecated, will be removed
      writeRequestsPerSecond: '%s{%s}' % [p.writeRequestsPerSecondMetric, p.writeRequestsPerSecondSelector],
      samplesPerSecond: 'sum(%(groupPrefixJobs)s:cortex_distributor_received_samples:rate5m{%(distributorMatcher)s})' % variables,
      exemplarsPerSecond: 'sum(%(groupPrefixJobs)s:cortex_distributor_received_exemplars:rate5m{%(distributorMatcher)s})' % variables,

      // Write failures rate as percentage of total requests.
      writeFailuresRate:: {
        local template = |||
          (
              # gRPC errors are not tracked as 5xx but "error".
              sum(%(countFailQuery)s)
              or
              # Handle the case no failure has been tracked yet.
              vector(0)
          )
          /
          sum(%(countQuery)s)
        |||,
        classic: template % {
          countFailQuery: utils.nativeClassicHistogramCountRate(p.writeRequestsPerSecondMetric, p.writeRequestsPerSecondSelector + ',status_code=~"5.*|error"').classic,
          countQuery: utils.nativeClassicHistogramCountRate(p.writeRequestsPerSecondMetric, p.writeRequestsPerSecondSelector).classic,
        },
        native: template % {
          countFailQuery: utils.nativeClassicHistogramCountRate(p.writeRequestsPerSecondMetric, p.writeRequestsPerSecondSelector + ',status_code=~"5.*|error"').native,
          countQuery: utils.nativeClassicHistogramCountRate(p.writeRequestsPerSecondMetric, p.writeRequestsPerSecondSelector).native,
        },
      },
    },

    query_frontend: {
      local p = self,
      readRequestsPerSecondMetric: 'cortex_request_duration_seconds',
      readRequestsPerSecondSelector: '%(queryFrontendMatcher)s, route=~"%(readHTTPRoutesRegex)s"' % variables,
      // deprecated, will be removed
      readRequestsPerSecond: '%s{%s}' % [p.readRequestsPerSecondMetric, p.readRequestsPerSecondSelector],
      instantQueriesPerSecond: 'sum(rate(cortex_request_duration_seconds_count{%(queryFrontendMatcher)s,route=~"(prometheus|api_prom)_api_v1_query"}[$__rate_interval]))' % variables,
      rangeQueriesPerSecond: 'sum(rate(cortex_request_duration_seconds_count{%(queryFrontendMatcher)s,route=~"(prometheus|api_prom)_api_v1_query_range"}[$__rate_interval]))' % variables,
      labelNamesQueriesPerSecond: 'sum(rate(cortex_request_duration_seconds_count{%(queryFrontendMatcher)s,route=~"(prometheus|api_prom)_api_v1_labels"}[$__rate_interval]))' % variables,
      labelValuesQueriesPerSecond: 'sum(rate(cortex_request_duration_seconds_count{%(queryFrontendMatcher)s,route=~"(prometheus|api_prom)_api_v1_label_name_values"}[$__rate_interval]))' % variables,
      seriesQueriesPerSecond: 'sum(rate(cortex_request_duration_seconds_count{%(queryFrontendMatcher)s,route=~"(prometheus|api_prom)_api_v1_series"}[$__rate_interval]))' % variables,
      remoteReadQueriesPerSecond: 'sum(rate(cortex_request_duration_seconds_count{%(queryFrontendMatcher)s,route=~"(prometheus|api_prom)_api_v1_read"}[$__rate_interval]))' % variables,
      metadataQueriesPerSecond: 'sum(rate(cortex_request_duration_seconds_count{%(queryFrontendMatcher)s,route=~"(prometheus|api_prom)_api_v1_metadata"}[$__rate_interval]))' % variables,
      exemplarsQueriesPerSecond: 'sum(rate(cortex_request_duration_seconds_count{%(queryFrontendMatcher)s,route=~"(prometheus|api_prom)_api_v1_query_exemplars"}[$__rate_interval]))' % variables,
      activeSeriesQueriesPerSecond: 'sum(rate(cortex_request_duration_seconds_count{%(queryFrontendMatcher)s,route="prometheus_api_v1_cardinality_active_series"}[$__rate_interval])) > 0' % variables,
      labelNamesCardinalityQueriesPerSecond: 'sum(rate(cortex_request_duration_seconds_count{%(queryFrontendMatcher)s,route="prometheus_api_v1_cardinality_label_names"}[$__rate_interval])) > 0' % variables,
      labelValuesCardinalityQueriesPerSecond: 'sum(rate(cortex_request_duration_seconds_count{%(queryFrontendMatcher)s,route="prometheus_api_v1_cardinality_label_values"}[$__rate_interval])) > 0' % variables,
      otherQueriesPerSecond: 'sum(rate(cortex_request_duration_seconds_count{%(queryFrontendMatcher)s,route=~"(prometheus|api_prom)_api_v1_.*",route!~".*(query|query_range|label.*|series|read|metadata|query_exemplars|cardinality_.*)"}[$__rate_interval]))' % variables,

      // Read failures rate as percentage of total requests.
      readFailuresRate:: {
        local template = |||
          (
              sum(%(countFailQuery)s)
              or
              # Handle the case no failure has been tracked yet.
              vector(0)
          )
          /
          sum(%(countQuery)s)
        |||,
        classic: template % {
          countFailQuery: utils.nativeClassicHistogramCountRate(p.readRequestsPerSecondMetric, p.readRequestsPerSecondSelector + ',status_code=~"5.*|error"').classic,
          countQuery: utils.nativeClassicHistogramCountRate(p.readRequestsPerSecondMetric, p.readRequestsPerSecondSelector).classic,
        },
        native: template % {
          countFailQuery: utils.nativeClassicHistogramCountRate(p.readRequestsPerSecondMetric, p.readRequestsPerSecondSelector + ',status_code=~"5.*|error"').native,
          countQuery: utils.nativeClassicHistogramCountRate(p.readRequestsPerSecondMetric, p.readRequestsPerSecondSelector).native,
        },
      },
    },

    ruler: {
      evaluations: {
        successPerSecond:
          |||
            sum(rate(cortex_prometheus_rule_evaluations_total{%(rulerMatcher)s}[$__rate_interval]))
            -
            sum(rate(cortex_prometheus_rule_evaluation_failures_total{%(rulerMatcher)s}[$__rate_interval]))
          ||| % variables,
        failurePerSecond: 'sum(rate(cortex_prometheus_rule_evaluation_failures_total{%(rulerMatcher)s}[$__rate_interval]))' % variables,
        missedIterationsPerSecond: 'sum(rate(cortex_prometheus_rule_group_iterations_missed_total{%(rulerMatcher)s}[$__rate_interval]))' % variables,
        latency:
          |||
            sum (rate(cortex_prometheus_rule_evaluation_duration_seconds_sum{%(rulerMatcher)s}[$__rate_interval]))
              /
            sum (rate(cortex_prometheus_rule_evaluation_duration_seconds_count{%(rulerMatcher)s}[$__rate_interval]))
          ||| % variables,

        // Rule evaluation failures rate as percentage of total requests.
        failuresRate: |||
          (
            (
                sum(rate(cortex_prometheus_rule_evaluation_failures_total{%(rulerMatcher)s}[$__rate_interval]))
                +
                # Consider missed evaluations as failures.
                sum(rate(cortex_prometheus_rule_group_iterations_missed_total{%(rulerMatcher)s}[$__rate_interval]))
            )
            or
            # Handle the case no failure has been tracked yet.
            vector(0)
          )
          /
          sum(rate(cortex_prometheus_rule_evaluations_total{%(rulerMatcher)s}[$__rate_interval]))
        ||| % variables,
      },
      notifications: {
        // Notifications / sec attempted to send to the Alertmanager.
        totalPerSecond: |||
          sum(rate(cortex_prometheus_notifications_sent_total{%(rulerMatcher)s}[$__rate_interval]))
        ||| % variables,

        // Notifications / sec successfully sent to the Alertmanager.
        successPerSecond: |||
          sum(rate(cortex_prometheus_notifications_sent_total{%(rulerMatcher)s}[$__rate_interval]))
            -
          sum(rate(cortex_prometheus_notifications_errors_total{%(rulerMatcher)s}[$__rate_interval]))
        ||| % variables,

        // Notifications / sec failed to be sent to the Alertmanager.
        failurePerSecond: |||
          sum(rate(cortex_prometheus_notifications_errors_total{%(rulerMatcher)s}[$__rate_interval]))
        ||| % variables,
      },
    },

    alertmanager: {
      notifications: {
        // Notifications / sec attempted to deliver by the Alertmanager to the receivers.
        totalPerSecond: |||
          sum(%(recordingRulePrefix)s_integration:cortex_alertmanager_notifications_total:rate5m{%(alertmanagerMatcher)s})
        ||| % variables,

        // Notifications / sec successfully delivered by the Alertmanager to the receivers.
        successPerSecond: |||
          sum(%(recordingRulePrefix)s_integration:cortex_alertmanager_notifications_total:rate5m{%(alertmanagerMatcher)s})
          -
          sum(%(recordingRulePrefix)s_integration:cortex_alertmanager_notifications_failed_total:rate5m{%(alertmanagerMatcher)s})
        ||| % variables,

        // Notifications / sec failed to be delivered by the Alertmanager to the receivers.
        failurePerSecond: |||
          sum(%(recordingRulePrefix)s_integration:cortex_alertmanager_notifications_failed_total:rate5m{%(alertmanagerMatcher)s})
        ||| % variables,
      },
    },

    storage: {
      successPerSecond: |||
        sum(rate(thanos_objstore_bucket_operations_total{%(namespaceMatcher)s}[$__rate_interval]))
        -
        sum(rate(thanos_objstore_bucket_operation_failures_total{%(namespaceMatcher)s}[$__rate_interval]))
      ||| % variables,
      failurePerSecond: |||
        sum(rate(thanos_objstore_bucket_operation_failures_total{%(namespaceMatcher)s}[$__rate_interval]))
      ||| % variables,

      // Object storage operation failures rate as percentage of total operations.
      failuresRate: |||
        sum(rate(thanos_objstore_bucket_operation_failures_total{%(namespaceMatcher)s}[$__rate_interval]))
        /
        sum(rate(thanos_objstore_bucket_operations_total{%(namespaceMatcher)s}[$__rate_interval]))
      ||| % variables,
    },
  },
}
