CREATE TABLE columns (
  state      Enum8('' = 0, 'idle' = 1, 'used' = 2) COMMENT 'state',

  -- android
  android_os_api_level String COMMENT 'android.os.api_level',
  android_state        Enum8('' = 0, 'created' = 1, 'background' = 2, 'foreground' = 3) COMMENT 'android.state',

  -- aws
  aws_ecs_cluster_arn    String COMMENT 'aws.ecs.cluster.arn',
  aws_ecs_container_arn  String COMMENT 'aws.ecs.container.arn',
  aws_ecs_launchtype     Enum8('' = 0, 'ec2' = 1, 'fargate' = 2) COMMENT 'aws.ecs.launchtype',
  aws_ecs_task_arn       String COMMENT 'aws.ecs.task.arn',
  aws_ecs_task_family    String COMMENT 'aws.ecs.task.family',
  aws_ecs_task_revision  String COMMENT 'aws.ecs.task.revision',
  aws_eks_cluster_arn    String COMMENT 'aws.eks.cluster.arn',
  aws_lambda_invoked_arn String COMMENT 'aws.lambda.invoked_arn',
  aws_log_group_arns     Array(String) COMMENT 'aws.log.group.arns',
  aws_log_group_names    Array(String) COMMENT 'aws.log.group.names',
  aws_log_stream_arns    Array(String) COMMENT 'aws.log.stream.arns',
  aws_log_stream_names   Array(String) COMMENT 'aws.log.stream.names',

  -- browser
  browser_brands   Array(String) COMMENT 'browser.brands',
  browser_language String COMMENT 'browser.language',
  browser_mobile   Bool COMMENT 'browser.mobile',
  browser_platform String COMMENT 'browser.platform',

  -- client
  client_address String COMMENT 'client.address',
  client_port    UInt16 COMMENT 'client.port',

  -- cloud
  cloud_account_id        String COMMENT 'cloud.account.id',
  cloud_availability_zone String COMMENT 'cloud.availability_zone',
  cloud_platform          String COMMENT 'cloud.platform',
  cloud_provider          String COMMENT 'cloud.provider',
  cloud_region            String COMMENT 'cloud.region',
  cloud_resource_id       String COMMENT 'cloud.resource_id',

  -- cloudevents
  cloudevents_event_id           String COMMENT 'cloudevents.event_id',
  cloudevents_event_source       String COMMENT 'cloudevents.event_source',
  cloudevents_event_spec_version String COMMENT 'cloudevents.event_spec_version',
  cloudevents_event_subject      String COMMENT 'cloudevents.event_subject',
  cloudevents_event_type         String COMMENT 'cloudevents.event_type',

  -- code
  code_column     Int64 COMMENT 'code.column',
  code_filepath   String COMMENT 'code.filepath',
  code_function   String COMMENT 'code.function',
  code_lineno     Int64 COMMENT 'code.lineno',
  code_namespace  String COMMENT 'code.namespace',
  code_stacktrace String COMMENT 'code.stacktrace',

  -- container
  container_command            String COMMENT 'container.command',
  container_command_args       Array(String) COMMENT 'container.command_args',
  container_command_line       String COMMENT 'container.command_line',
  container_id                 String COMMENT 'container.id',
  container_image_id           String COMMENT 'container.image.id',
  container_image_name         String COMMENT 'container.image.name',
  container_image_repo_digests Array(String) COMMENT 'container.image.repo_digests',
  container_image_tags         Array(String) COMMENT 'container.image.tags',
  container_labels             String COMMENT 'container.labels',
  container_name               String COMMENT 'container.name',
  container_runtime            String COMMENT 'container.runtime',

  -- db
  db_cassandra_consistency_level           Enum8('' = 0, 'all' = 1, 'each_quorum' = 2, 'quorum' = 3, 'local_quorum' = 4, 'one' = 5, 'two' = 6, 'three' = 7, 'local_one' = 8, 'any' = 9, 'serial' = 10, 'local_serial' = 11) COMMENT 'db.cassandra.consistency_level',
  db_cassandra_coordinator_dc              String COMMENT 'db.cassandra.coordinator.dc',
  db_cassandra_coordinator_id              String COMMENT 'db.cassandra.coordinator.id',
  db_cassandra_idempotence                 Bool COMMENT 'db.cassandra.idempotence',
  db_cassandra_page_size                   Int64 COMMENT 'db.cassandra.page_size',
  db_cassandra_speculative_execution_count Int64 COMMENT 'db.cassandra.speculative_execution_count',
  db_cassandra_table                       String COMMENT 'db.cassandra.table',
  db_connection_string                     String COMMENT 'db.connection_string',
  db_cosmosdb_client_id                    String COMMENT 'db.cosmosdb.client_id',
  db_cosmosdb_connection_mode              Enum8('' = 0, 'gateway' = 1, 'direct' = 2) COMMENT 'db.cosmosdb.connection_mode',
  db_cosmosdb_container                    String COMMENT 'db.cosmosdb.container',
  db_cosmosdb_operation_type               String COMMENT 'db.cosmosdb.operation_type',
  db_cosmosdb_request_charge               Float64 COMMENT 'db.cosmosdb.request_charge',
  db_cosmosdb_request_content_length       Int64 COMMENT 'db.cosmosdb.request_content_length',
  db_cosmosdb_status_code                  Int64 COMMENT 'db.cosmosdb.status_code',
  db_cosmosdb_sub_status_code              Int64 COMMENT 'db.cosmosdb.sub_status_code',
  db_elasticsearch_cluster_name            String COMMENT 'db.elasticsearch.cluster.name',
  db_elasticsearch_node_name               String COMMENT 'db.elasticsearch.node.name',
  db_elasticsearch_path_parts              String COMMENT 'db.elasticsearch.path_parts',
  db_instance_id                           String COMMENT 'db.instance.id',
  db_jdbc_driver_classname                 String COMMENT 'db.jdbc.driver_classname',
  db_mongodb_collection                    String COMMENT 'db.mongodb.collection',
  db_mssql_instance_name                   String COMMENT 'db.mssql.instance_name',
  db_name                                  String COMMENT 'db.name',
  db_operation                             String COMMENT 'db.operation',
  db_redis_database_index                  Int64 COMMENT 'db.redis.database_index',
  db_sql_table                             String COMMENT 'db.sql.table',
  db_statement                             String COMMENT 'db.statement',
  db_system                                String COMMENT 'db.system',
  db_user                                  String COMMENT 'db.user',

  -- deployment
  deployment_environment String COMMENT 'deployment.environment',

  -- destination
  destination_address String COMMENT 'destination.address',
  destination_port    UInt16 COMMENT 'destination.port',

  -- device
  device_id               String COMMENT 'device.id',
  device_manufacturer     String COMMENT 'device.manufacturer',
  device_model_identifier String COMMENT 'device.model.identifier',
  device_model_name       String COMMENT 'device.model.name',

  -- disk
  disk_io_direction Enum8('' = 0, 'read' = 1, 'write' = 2) COMMENT 'disk.io.direction',

  -- enduser
  enduser_id    String COMMENT 'enduser.id',
  enduser_role  String COMMENT 'enduser.role',
  enduser_scope String COMMENT 'enduser.scope',

  -- error
  error_type String COMMENT 'error.type',

  -- event
  event_name String COMMENT 'event.name',

  -- exception
  exception_escaped    Bool COMMENT 'exception.escaped',
  exception_message    String COMMENT 'exception.message',
  exception_stacktrace String COMMENT 'exception.stacktrace',
  exception_type       String COMMENT 'exception.type',

  -- faas
  faas_coldstart           Bool COMMENT 'faas.coldstart',
  faas_cron                String COMMENT 'faas.cron',
  faas_document_collection String COMMENT 'faas.document.collection',
  faas_document_name       String COMMENT 'faas.document.name',
  faas_document_operation  String COMMENT 'faas.document.operation',
  faas_document_time       DateTime COMMENT 'faas.document.time',
  faas_instance            String COMMENT 'faas.instance',
  faas_invocation_id       String COMMENT 'faas.invocation_id',
  faas_invoked_name        String COMMENT 'faas.invoked_name',
  faas_invoked_provider    String COMMENT 'faas.invoked_provider',
  faas_invoked_region      String COMMENT 'faas.invoked_region',
  faas_max_memory          Int64 COMMENT 'faas.max_memory',
  faas_name                String COMMENT 'faas.name',
  faas_time                DateTime COMMENT 'faas.time',
  faas_trigger             Enum8('' = 0, 'datasource' = 1, 'http' = 2, 'pubsub' = 3, 'timer' = 4, 'other' = 5) COMMENT 'faas.trigger',
  faas_version             String COMMENT 'faas.version',

  -- feature_flag
  feature_flag_key           String COMMENT 'feature_flag.key',
  feature_flag_provider_name String COMMENT 'feature_flag.provider_name',
  feature_flag_variant       String COMMENT 'feature_flag.variant',

  -- gcp
  gcp_cloud_run_job_execution  String COMMENT 'gcp.cloud_run.job.execution',
  gcp_cloud_run_job_task_index Int64 COMMENT 'gcp.cloud_run.job.task_index',
  gcp_gce_instance_hostname    String COMMENT 'gcp.gce.instance.hostname',
  gcp_gce_instance_name        String COMMENT 'gcp.gce.instance.name',

  -- heroku
  heroku_app_id                     String COMMENT 'heroku.app.id',
  heroku_release_commit             String COMMENT 'heroku.release.commit',
  heroku_release_creation_timestamp String COMMENT 'heroku.release.creation_timestamp',

  -- host
  host_arch              String COMMENT 'host.arch',
  host_cpu_cache_l2_size Int64 COMMENT 'host.cpu.cache.l2.size',
  host_cpu_family        String COMMENT 'host.cpu.family',
  host_cpu_model_id      String COMMENT 'host.cpu.model.id',
  host_cpu_model_name    String COMMENT 'host.cpu.model.name',
  host_cpu_stepping      Int64 COMMENT 'host.cpu.stepping',
  host_cpu_vendor_id     String COMMENT 'host.cpu.vendor.id',
  host_id                String COMMENT 'host.id',
  host_image_id          String COMMENT 'host.image.id',
  host_image_name        String COMMENT 'host.image.name',
  host_image_version     String COMMENT 'host.image.version',
  host_ip                Array(String) COMMENT 'host.ip',
  host_mac               Array(String) COMMENT 'host.mac',
  host_name              String COMMENT 'host.name',
  host_type              String COMMENT 'host.type',

  -- http
  http_request_body_size       Int64 COMMENT 'http.request.body.size',
  http_request_header          Array(String) COMMENT 'http.request.header',
  http_request_method          String COMMENT 'http.request.method',
  http_request_method_original String COMMENT 'http.request.method_original',
  http_request_resend_count    Int64 COMMENT 'http.request.resend_count',
  http_response_body_size      Int64 COMMENT 'http.response.body.size',
  http_response_header         Array(String) COMMENT 'http.response.header',
  http_response_status_code    Int64 COMMENT 'http.response.status_code',
  http_route                   String COMMENT 'http.route',

  -- ios
  ios_state  Enum8('' = 0, 'active' = 1, 'inactive' = 2, 'background' = 3, 'foreground' = 4, 'terminate' = 5) COMMENT 'ios.state',

  -- jvm
  jvm_buffer_pool_name String COMMENT 'jvm.buffer.pool.name',
  jvm_gc_action        String COMMENT 'jvm.gc.action',
  jvm_gc_name          String COMMENT 'jvm.gc.name',
  jvm_memory_pool_name String COMMENT 'jvm.memory.pool.name',
  jvm_memory_type      Enum8('' = 0, 'heap' = 1, 'non_heap' = 2) COMMENT 'jvm.memory.type',
  jvm_thread_daemon    Bool COMMENT 'jvm.thread.daemon',
  jvm_thread_state     Enum8('' = 0, 'new' = 1, 'runnable' = 2, 'blocked' = 3, 'waiting' = 4, 'timed_waiting' = 5, 'terminated' = 6) COMMENT 'jvm.thread.state',

  -- k8s
  k8s_cluster_name            String COMMENT 'k8s.cluster.name',
  k8s_cluster_uid             UUID COMMENT 'k8s.cluster.uid',
  k8s_container_name          String COMMENT 'k8s.container.name',
  k8s_container_restart_count Int64 COMMENT 'k8s.container.restart_count',
  k8s_cronjob_name            String COMMENT 'k8s.cronjob.name',
  k8s_cronjob_uid             UUID COMMENT 'k8s.cronjob.uid',
  k8s_daemonset_name          String COMMENT 'k8s.daemonset.name',
  k8s_daemonset_uid           UUID COMMENT 'k8s.daemonset.uid',
  k8s_deployment_name         String COMMENT 'k8s.deployment.name',
  k8s_deployment_uid          UUID COMMENT 'k8s.deployment.uid',
  k8s_job_name                String COMMENT 'k8s.job.name',
  k8s_job_uid                 UUID COMMENT 'k8s.job.uid',
  k8s_namespace_name          String COMMENT 'k8s.namespace.name',
  k8s_node_name               String COMMENT 'k8s.node.name',
  k8s_node_uid                UUID COMMENT 'k8s.node.uid',
  k8s_pod_labels              String COMMENT 'k8s.pod.labels',
  k8s_pod_name                String COMMENT 'k8s.pod.name',
  k8s_pod_uid                 UUID COMMENT 'k8s.pod.uid',
  k8s_replicaset_name         String COMMENT 'k8s.replicaset.name',
  k8s_replicaset_uid          UUID COMMENT 'k8s.replicaset.uid',
  k8s_statefulset_name        String COMMENT 'k8s.statefulset.name',
  k8s_statefulset_uid         UUID COMMENT 'k8s.statefulset.uid',

  -- log
  log_file_name          String COMMENT 'log.file.name',
  log_file_name_resolved String COMMENT 'log.file.name_resolved',
  log_file_path          String COMMENT 'log.file.path',
  log_file_path_resolved String COMMENT 'log.file.path_resolved',
  log_iostream           Enum8('' = 0, 'stdout' = 1, 'stderr' = 2) COMMENT 'log.iostream',
  log_record_uid         String COMMENT 'log.record.uid',

  -- message
  message_compressed_size   Int64 COMMENT 'message.compressed_size',
  message_id                Int64 COMMENT 'message.id',
  message_type              Enum8('' = 0, 'SENT' = 1, 'RECEIVED' = 2) COMMENT 'message.type',
  message_uncompressed_size Int64 COMMENT 'message.uncompressed_size',

  -- messaging
  messaging_batch_message_count                 Int64 COMMENT 'messaging.batch.message_count',
  messaging_client_id                           String COMMENT 'messaging.client_id',
  messaging_destination_anonymous               Bool COMMENT 'messaging.destination.anonymous',
  messaging_destination_name                    String COMMENT 'messaging.destination.name',
  messaging_destination_publish_anonymous       Bool COMMENT 'messaging.destination_publish.anonymous',
  messaging_destination_publish_name            String COMMENT 'messaging.destination_publish.name',
  messaging_destination_template                String COMMENT 'messaging.destination.template',
  messaging_destination_temporary               Bool COMMENT 'messaging.destination.temporary',
  messaging_gcp_pubsub_message_ordering_key     String COMMENT 'messaging.gcp_pubsub.message.ordering_key',
  messaging_kafka_consumer_group                String COMMENT 'messaging.kafka.consumer.group',
  messaging_kafka_destination_partition         Int64 COMMENT 'messaging.kafka.destination.partition',
  messaging_kafka_message_key                   String COMMENT 'messaging.kafka.message.key',
  messaging_kafka_message_offset                Int64 COMMENT 'messaging.kafka.message.offset',
  messaging_kafka_message_tombstone             Bool COMMENT 'messaging.kafka.message.tombstone',
  messaging_message_body_size                   Int64 COMMENT 'messaging.message.body.size',
  messaging_message_conversation_id             String COMMENT 'messaging.message.conversation_id',
  messaging_message_envelope_size               Int64 COMMENT 'messaging.message.envelope.size',
  messaging_message_id                          String COMMENT 'messaging.message.id',
  messaging_operation                           String COMMENT 'messaging.operation',
  messaging_rabbitmq_destination_routing_key    String COMMENT 'messaging.rabbitmq.destination.routing_key',
  messaging_rocketmq_client_group               String COMMENT 'messaging.rocketmq.client_group',
  messaging_rocketmq_consumption_model          Enum8('' = 0, 'clustering' = 1, 'broadcasting' = 2) COMMENT 'messaging.rocketmq.consumption_model',
  messaging_rocketmq_message_delay_time_level   Int64 COMMENT 'messaging.rocketmq.message.delay_time_level',
  messaging_rocketmq_message_delivery_timestamp Int64 COMMENT 'messaging.rocketmq.message.delivery_timestamp',
  messaging_rocketmq_message_group              String COMMENT 'messaging.rocketmq.message.group',
  messaging_rocketmq_message_keys               Array(String) COMMENT 'messaging.rocketmq.message.keys',
  messaging_rocketmq_message_tag                String COMMENT 'messaging.rocketmq.message.tag',
  messaging_rocketmq_message_type               Enum8('' = 0, 'normal' = 1, 'fifo' = 2, 'delay' = 3, 'transaction' = 4) COMMENT 'messaging.rocketmq.message.type',
  messaging_rocketmq_namespace                  String COMMENT 'messaging.rocketmq.namespace',
  messaging_system                              String COMMENT 'messaging.system',

  -- network
  network_carrier_icc        String COMMENT 'network.carrier.icc',
  network_carrier_mcc        String COMMENT 'network.carrier.mcc',
  network_carrier_mnc        String COMMENT 'network.carrier.mnc',
  network_carrier_name       String COMMENT 'network.carrier.name',
  network_connection_subtype String COMMENT 'network.connection.subtype',
  network_connection_type    String COMMENT 'network.connection.type',
  network_io_direction       Enum8('' = 0, 'transmit' = 1, 'receive' = 2) COMMENT 'network.io.direction',
  network_local_address      String COMMENT 'network.local.address',
  network_local_port         UInt16 COMMENT 'network.local.port',
  network_peer_address       String COMMENT 'network.peer.address',
  network_peer_port          UInt16 COMMENT 'network.peer.port',
  network_protocol_name      String COMMENT 'network.protocol.name',
  network_protocol_version   String COMMENT 'network.protocol.version',
  network_transport          String COMMENT 'network.transport',
  network_type               String COMMENT 'network.type',

  -- oci
  oci_manifest_digest String COMMENT 'oci.manifest.digest',

  -- opentracing
  opentracing_ref_type Enum8('' = 0, 'child_of' = 1, 'follows_from' = 2) COMMENT 'opentracing.ref_type',

  -- os
  os_build_id    String COMMENT 'os.build_id',
  os_description String COMMENT 'os.description',
  os_name        String COMMENT 'os.name',
  os_type        String COMMENT 'os.type',
  os_version     String COMMENT 'os.version',

  -- otel
  otel_scope_name         String COMMENT 'otel.scope.name',
  otel_scope_version      String COMMENT 'otel.scope.version',
  otel_status_code        Enum8('' = 0, 'OK' = 1, 'ERROR' = 2) COMMENT 'otel.status_code',
  otel_status_description String COMMENT 'otel.status_description',

  -- peer
  peer_service String COMMENT 'peer.service',

  -- pool
  pool_name  String COMMENT 'pool.name',

  -- process
  process_command             String COMMENT 'process.command',
  process_command_args        Array(String) COMMENT 'process.command_args',
  process_command_line        String COMMENT 'process.command_line',
  process_executable_name     String COMMENT 'process.executable.name',
  process_executable_path     String COMMENT 'process.executable.path',
  process_owner               String COMMENT 'process.owner',
  process_parent_pid          Int64 COMMENT 'process.parent_pid',
  process_pid                 Int64 COMMENT 'process.pid',
  process_runtime_description String COMMENT 'process.runtime.description',
  process_runtime_name        String COMMENT 'process.runtime.name',
  process_runtime_version     String COMMENT 'process.runtime.version',

  -- rpc
  rpc_connect_rpc_error_code        Enum8('' = 0, 'cancelled' = 1, 'unknown' = 2, 'invalid_argument' = 3, 'deadline_exceeded' = 4, 'not_found' = 5, 'already_exists' = 6, 'permission_denied' = 7, 'resource_exhausted' = 8, 'failed_precondition' = 9, 'aborted' = 10, 'out_of_range' = 11, 'unimplemented' = 12, 'internal' = 13, 'unavailable' = 14, 'data_loss' = 15, 'unauthenticated' = 16) COMMENT 'rpc.connect_rpc.error_code',
  rpc_connect_rpc_request_metadata  Array(String) COMMENT 'rpc.connect_rpc.request.metadata',
  rpc_connect_rpc_response_metadata Array(String) COMMENT 'rpc.connect_rpc.response.metadata',
  rpc_grpc_request_metadata         Array(String) COMMENT 'rpc.grpc.request.metadata',
  rpc_grpc_response_metadata        Array(String) COMMENT 'rpc.grpc.response.metadata',
  rpc_grpc_status_code              UInt8 COMMENT 'rpc.grpc.status_code',
  rpc_jsonrpc_error_code            Int64 COMMENT 'rpc.jsonrpc.error_code',
  rpc_jsonrpc_error_message         String COMMENT 'rpc.jsonrpc.error_message',
  rpc_jsonrpc_request_id            String COMMENT 'rpc.jsonrpc.request_id',
  rpc_jsonrpc_version               String COMMENT 'rpc.jsonrpc.version',
  rpc_method                        String COMMENT 'rpc.method',
  rpc_service                       String COMMENT 'rpc.service',
  rpc_system                        String COMMENT 'rpc.system',

  -- server
  server_address String COMMENT 'server.address',
  server_port    UInt16 COMMENT 'server.port',

  -- service
  service_instance_id String COMMENT 'service.instance.id',
  service_name        String COMMENT 'service.name',
  service_namespace   String COMMENT 'service.namespace',
  service_version     String COMMENT 'service.version',

  -- session
  session_id          String COMMENT 'session.id',
  session_previous_id String COMMENT 'session.previous_id',

  -- source
  source_address String COMMENT 'source.address',
  source_port    UInt16 COMMENT 'source.port',

  -- system
  system_cpu_logical_number    Int64 COMMENT 'system.cpu.logical_number',
  system_cpu_state             String COMMENT 'system.cpu.state',
  system_device                String COMMENT 'system.device',
  system_filesystem_mode       String COMMENT 'system.filesystem.mode',
  system_filesystem_mountpoint String COMMENT 'system.filesystem.mountpoint',
  system_filesystem_state      Enum8('' = 0, 'used' = 1, 'free' = 2, 'reserved' = 3) COMMENT 'system.filesystem.state',
  system_filesystem_type       String COMMENT 'system.filesystem.type',
  system_memory_state          String COMMENT 'system.memory.state',
  system_network_state         Enum8('' = 0, 'close' = 1, 'close_wait' = 2, 'closing' = 3, 'delete' = 4, 'established' = 5, 'fin_wait_1' = 6, 'fin_wait_2' = 7, 'last_ack' = 8, 'listen' = 9, 'syn_recv' = 10, 'syn_sent' = 11, 'time_wait' = 12) COMMENT 'system.network.state',
  system_paging_direction      Enum8('' = 0, 'in' = 1, 'out' = 2) COMMENT 'system.paging.direction',
  system_paging_state          Enum8('' = 0, 'used' = 1, 'free' = 2) COMMENT 'system.paging.state',
  system_paging_type           Enum8('' = 0, 'major' = 1, 'minor' = 2) COMMENT 'system.paging.type',
  system_processes_status      String COMMENT 'system.processes.status',

  -- telemetry
  telemetry_distro_name    String COMMENT 'telemetry.distro.name',
  telemetry_distro_version String COMMENT 'telemetry.distro.version',
  telemetry_sdk_language   String COMMENT 'telemetry.sdk.language',
  telemetry_sdk_name       String COMMENT 'telemetry.sdk.name',
  telemetry_sdk_version    String COMMENT 'telemetry.sdk.version',

  -- thread
  thread_id   Int64 COMMENT 'thread.id',
  thread_name String COMMENT 'thread.name',

  -- tls
  tls_cipher                   String COMMENT 'tls.cipher',
  tls_client_certificate       String COMMENT 'tls.client.certificate',
  tls_client_certificate_chain Array(String) COMMENT 'tls.client.certificate_chain',
  tls_client_hash_md5          String COMMENT 'tls.client.hash.md5',
  tls_client_hash_sha1         String COMMENT 'tls.client.hash.sha1',
  tls_client_hash_sha256       String COMMENT 'tls.client.hash.sha256',
  tls_client_issuer            String COMMENT 'tls.client.issuer',
  tls_client_ja3               String COMMENT 'tls.client.ja3',
  tls_client_not_after         String COMMENT 'tls.client.not_after',
  tls_client_not_before        String COMMENT 'tls.client.not_before',
  tls_client_server_name       String COMMENT 'tls.client.server_name',
  tls_client_subject           String COMMENT 'tls.client.subject',
  tls_client_supported_ciphers Array(String) COMMENT 'tls.client.supported_ciphers',
  tls_curve                    String COMMENT 'tls.curve',
  tls_established              Bool COMMENT 'tls.established',
  tls_next_protocol            String COMMENT 'tls.next_protocol',
  tls_protocol_name            String COMMENT 'tls.protocol.name',
  tls_protocol_version         String COMMENT 'tls.protocol.version',
  tls_resumed                  Bool COMMENT 'tls.resumed',
  tls_server_certificate       String COMMENT 'tls.server.certificate',
  tls_server_certificate_chain Array(String) COMMENT 'tls.server.certificate_chain',
  tls_server_hash_md5          String COMMENT 'tls.server.hash.md5',
  tls_server_hash_sha1         String COMMENT 'tls.server.hash.sha1',
  tls_server_hash_sha256       String COMMENT 'tls.server.hash.sha256',
  tls_server_issuer            String COMMENT 'tls.server.issuer',
  tls_server_ja3s              String COMMENT 'tls.server.ja3s',
  tls_server_not_after         String COMMENT 'tls.server.not_after',
  tls_server_not_before        String COMMENT 'tls.server.not_before',
  tls_server_subject           String COMMENT 'tls.server.subject',

  -- url
  url_fragment String COMMENT 'url.fragment',
  url_full     String COMMENT 'url.full',
  url_path     String COMMENT 'url.path',
  url_query    String COMMENT 'url.query',
  url_scheme   String COMMENT 'url.scheme',

  -- user_agent
  user_agent_original String COMMENT 'user_agent.original',

  -- webengine
  webengine_description String COMMENT 'webengine.description',
  webengine_name        String COMMENT 'webengine.name',
  webengine_version     String COMMENT 'webengine.version'
) ENGINE Null;