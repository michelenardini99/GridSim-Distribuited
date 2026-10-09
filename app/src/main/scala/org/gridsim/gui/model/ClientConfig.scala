package org.gridsim.gui.model

/**
 * Configuration for the remote UI client, typically provided via CLI args.
 *
 * @param apiEndpoint The base HTTP endpoint for the Simulation Control Agent API (e.g., http://localhost:8080)
 * @param kafkaBootstrapServers The Kafka bootstrap servers for telemetry (e.g., localhost:9092)
 */
case class ClientConfig(
  apiEndpoint: String,
  kafkaBootstrapServers: String
)
