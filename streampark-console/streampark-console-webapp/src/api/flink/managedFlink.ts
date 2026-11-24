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

import { defHttp } from '/@/utils/http/axios';
import { getUserTeamId } from '/@/utils';

export interface Spec {
  type: 'SQL' | 'JAR';
  batchMode?: boolean;
  priority?: number;
  schedulePolicy?: 'DRF' | 'GANG';
  scheduleTimeout?: number;
  sql?: string;
  artifactId?: string;
  udfArtifactId?: string;
  dependencyIds?: string[];
  mainClass?: string;
  args: string[];
  configuration: Record<string, string>;
}

export interface Job {
  appId: string;
  environmentId: string;
  engineRef: string;
  draftRevision: string;
  activeReleaseId?: string;
  currentRunId?: string;
  pendingOperationId?: string;
  syncStatus: string;
  cloudBound?: boolean;
  directSubmission?: boolean;
  observedAt?: number | string;
  spec: Spec;
}

export interface Environment {
  id: string;
  name: string;
  region: string;
  provider: string;
  configVersion: number;
  enabled: boolean | number;
  target: string;
}

export interface Operation {
  id: string;
  command_type: string;
  status: string;
  steps?: {
    step_index: number;
    action: string;
    state: string;
    error_code?: string;
    request_id?: string;
  }[];
}

export interface Release {
  id: string;
  release_no: string;
  status: string;
  provider_version?: string;
}

export interface Run {
  id: string;
  release_id: string;
  run_state: string;
  origin: string;
  provider_instance_id: string;
}

export interface Snapshot {
  id: string;
  release_id: string;
  state: string;
  restorable: boolean | number;
}

export type Capabilities = Record<string, string>;

const base = '/flink/managed';
const quiet = { handleErrorsLocally: true, errorMessageMode: 'none' as const, formatDate: false };

export function managedGet<T>(
  path: string,
  params: Record<string, number | string | undefined> = {},
) {
  return defHttp.get<T>(
    { url: base + path, params: { ...params, teamId: getUserTeamId() } },
    quiet,
  );
}

export function managedPost<T>(path: string, data: Record<string, unknown>, key?: string) {
  const url = base + path + '?teamId=' + encodeURIComponent(getUserTeamId() || '');
  return defHttp.postJson<T>(
    { url, data, headers: key ? { 'Idempotency-Key': key } : undefined },
    quiet,
  );
}

export function managedPatch<T>(path: string, data: Record<string, unknown>) {
  const url = base + path + '?teamId=' + encodeURIComponent(getUserTeamId() || '');
  return defHttp.request<T>(
    { url, data, method: 'PATCH', headers: { 'Content-Type': 'application/json' } },
    quiet,
  );
}

export async function uploadManagedJar(app: string, file: File) {
  const url =
    base + '/apps/' + app + '/artifacts?teamId=' + encodeURIComponent(getUserTeamId() || '');
  return defHttp.post<string>(
    {
      url,
      params: { file },
      headers: { 'Content-Type': 'multipart/form-data' },
      timeout: 600_000,
    },
    quiet,
  );
}
