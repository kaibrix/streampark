/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

CREATE TABLE IF NOT EXISTS t_flink_cloud_connection (
  id varchar(36) PRIMARY KEY,
  team_id bigint NOT NULL,
  name varchar(128) NOT NULL,
  provider varchar(32) NOT NULL,
  account_ref varchar(128) NOT NULL,
  credential text,
  credential_version int NOT NULL DEFAULT 1,
  created_by bigint NOT NULL,
  created_at bigint NOT NULL,
  UNIQUE (team_id, name)
);

CREATE TABLE IF NOT EXISTS t_flink_managed_env (
  id varchar(36) PRIMARY KEY,
  team_id bigint NOT NULL,
  connection_id varchar(36) NOT NULL REFERENCES t_flink_cloud_connection (id),
  name varchar(128) NOT NULL,
  region varchar(64) NOT NULL,
  target text NOT NULL,
  config_version int NOT NULL DEFAULT 1,
  capability_report text,
  capability_credential_version int,
  capability_observed_at bigint,
  enabled boolean NOT NULL DEFAULT true,
  UNIQUE (team_id, name)
);

CREATE TABLE IF NOT EXISTS t_flink_managed_job (
  app_id bigint PRIMARY KEY REFERENCES t_flink_app (id),
  team_id bigint NOT NULL,
  environment_id varchar(36) NOT NULL REFERENCES t_flink_managed_env (id),
  engine_ref varchar(128) NOT NULL,
  draft_revision bigint NOT NULL,
  draft_spec text NOT NULL,
  scope_key varchar(64) NOT NULL,
  provider_draft_id varchar(191),
  provider_job_id varchar(191),
  active_release_id varchar(36),
  current_run_id varchar(36),
  pending_operation_id varchar(36),
  sync_status varchar(24) NOT NULL DEFAULT 'STALE',
  next_observe_at bigint NOT NULL DEFAULT 0,
  missing_reads int NOT NULL DEFAULT 0,
  observed_at bigint,
  archived boolean NOT NULL DEFAULT false,
  UNIQUE (scope_key, provider_job_id)
);

CREATE TABLE IF NOT EXISTS t_flink_managed_artifact (
  id varchar(36) PRIMARY KEY,
  app_id bigint NOT NULL REFERENCES t_flink_managed_job (app_id),
  digest varchar(64) NOT NULL,
  size_bytes bigint NOT NULL,
  object_ref varchar(768) NOT NULL,
  main_class varchar(255),
  created_at bigint NOT NULL,
  UNIQUE (app_id, digest)
);

CREATE TABLE IF NOT EXISTS t_flink_managed_release (
  id varchar(36) PRIMARY KEY,
  app_id bigint NOT NULL REFERENCES t_flink_managed_job (app_id),
  release_no bigint NOT NULL,
  draft_revision bigint NOT NULL,
  config_version int NOT NULL,
  spec text NOT NULL,
  spec_hash varchar(64) NOT NULL,
  target text NOT NULL,
  provider_version varchar(191),
  artifact_ref varchar(768),
  status varchar(24) NOT NULL,
  created_at bigint NOT NULL,
  UNIQUE (app_id, release_no)
);

CREATE TABLE IF NOT EXISTS t_flink_managed_run (
  id varchar(36) PRIMARY KEY,
  app_id bigint NOT NULL REFERENCES t_flink_managed_job (app_id),
  release_id varchar(36) REFERENCES t_flink_managed_release (id),
  provider_instance_id varchar(191) NOT NULL,
  runtime_application_id varchar(191),
  run_state varchar(24) NOT NULL,
  raw_state varchar(64),
  origin varchar(16) NOT NULL,
  started_at bigint NOT NULL,
  ended_at bigint,
  observed_at bigint NOT NULL,
  UNIQUE (app_id, provider_instance_id)
);

CREATE TABLE IF NOT EXISTS t_flink_managed_operation (
  id varchar(36) PRIMARY KEY,
  app_id bigint NOT NULL REFERENCES t_flink_managed_job (app_id),
  team_id bigint NOT NULL,
  idempotency_key varchar(128) NOT NULL,
  request_hash varchar(64) NOT NULL,
  command_type varchar(24) NOT NULL,
  command text NOT NULL,
  release_id varchar(36) REFERENCES t_flink_managed_release (id),
  status varchar(24) NOT NULL,
  step_index int NOT NULL DEFAULT 0,
  deadline_at bigint NOT NULL,
  next_run_at bigint NOT NULL,
  lease_owner varchar(36),
  lease_until bigint,
  lease_epoch bigint NOT NULL DEFAULT 0,
  created_by bigint NOT NULL,
  created_at bigint NOT NULL,
  updated_at bigint NOT NULL,
  UNIQUE (team_id, app_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS t_flink_managed_operation_step (
  operation_id varchar(36) NOT NULL REFERENCES t_flink_managed_operation (id),
  step_index int NOT NULL,
  action varchar(32) NOT NULL,
  state varchar(24) NOT NULL,
  request_id varchar(128),
  result text,
  error_code varchar(64),
  started_at bigint NOT NULL,
  finished_at bigint,
  PRIMARY KEY (operation_id, step_index)
);

CREATE TABLE IF NOT EXISTS t_flink_managed_snapshot (
  id varchar(36) PRIMARY KEY,
  app_id bigint NOT NULL REFERENCES t_flink_managed_job (app_id),
  run_id varchar(36) NOT NULL REFERENCES t_flink_managed_run (id),
  release_id varchar(36) REFERENCES t_flink_managed_release (id),
  operation_id varchar(36) REFERENCES t_flink_managed_operation (id),
  provider_record_id varchar(191) NOT NULL,
  restore_ref varchar(191),
  state varchar(24) NOT NULL,
  restorable boolean NOT NULL,
  created_at bigint NOT NULL,
  observed_at bigint NOT NULL,
  UNIQUE (app_id, provider_record_id)
);

CREATE INDEX IF NOT EXISTS idx_managed_operation_due ON t_flink_managed_operation (status, next_run_at);
CREATE INDEX IF NOT EXISTS idx_managed_job_team ON t_flink_managed_job (team_id, environment_id);
CREATE INDEX IF NOT EXISTS idx_managed_run_history ON t_flink_managed_run (app_id, started_at);
CREATE INDEX IF NOT EXISTS idx_managed_snapshot_history ON t_flink_managed_snapshot (app_id, created_at);

-- Managed Flink logs require a separately assignable permission.
INSERT INTO public.t_menu
  (menu_id, parent_id, menu_name, perms, type, display, create_time, modify_time)
SELECT 120219, 120200, 'managed app logs', 'app:log', 1, true, now(), now()
WHERE NOT EXISTS (SELECT 1 FROM public.t_menu WHERE menu_id = 120219 OR perms = 'app:log');
