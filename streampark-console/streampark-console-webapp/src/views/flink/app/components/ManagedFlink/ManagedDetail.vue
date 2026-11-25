<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
<script setup lang="ts">
  import {
    Alert,
    Button,
    Checkbox,
    Drawer,
    Form,
    Input,
    InputNumber,
    List,
    Popconfirm,
    Select,
    Space,
    Tabs,
    Tag,
  } from 'ant-design-vue';
  import { computed, onMounted, onUnmounted, reactive, ref, watch } from 'vue';
  import { useI18n } from '/@/hooks/web/useI18n';
  import { usePermission } from '/@/hooks/web/usePermission';
  import {
    managedGet,
    managedPatch,
    managedPost,
    uploadManagedJar,
    type Job,
    type Spec,
    type Operation,
    type Release,
    type Run,
    type Snapshot,
  } from '/@/api/flink/managedFlink';
  import { buildUUID } from '/@/utils/uuid';
  import { format } from '../../FlinkSqlFormatter';

  const FormItem = Form.Item;
  const ListItem = List.Item;
  const TabPane = Tabs.TabPane;
  const Textarea = Input.TextArea;

  const props = defineProps<{ appId: string; name?: string }>();
  const { t } = useI18n();
  const { hasPermission } = usePermission();
  const base = `/apps/${props.appId}`;
  const job = ref<Job>();
  const spec = ref<Spec>();
  const argText = ref('');
  const configurationText = ref('');
  const buildSource = reactive({ projectId: '', module: '', jar: '' });
  const editingRevision = ref<string>();
  const savedSpec = ref('');
  const savedConfigurationText = ref('');
  const capabilities = computed(() => job.value?.capabilities || {});
  const runtimeCapabilities = computed(() => job.value?.runtimeCapabilities || {});
  const cloud = ref<{
    status: string;
    jobId?: string;
    jobType?: string;
    startMethods?: string[];
    instances?: { Id: string; State: string; StartTime: string }[];
    savepoints?: { SavepointId: string; Status: string; CreateTime: string }[];
    page?: number;
    hasNext?: boolean;
  }>();

  const cloudInstance = ref<{
    status: string;
    id?: string;
    state?: string;
    engine?: string;
    runtimeId?: string;
    dashboard?: string;
  }>();

  function readCloudInstance(id: string) {
    void action(async () => {
      cloudInstance.value = undefined;
      cloudInstance.value = await managedGet(base + '/cloud/instances/' + encodeURIComponent(id));
    });
  }

  function readCloud(page = 1) {
    void action(async () => {
      cloud.value = undefined;
      cloudInstance.value = undefined;
      cloud.value = await managedGet(base + '/cloud', { page });
    });
  }
  const releases = ref<Release[]>([]);
  const runs = ref<Run[]>([]);
  const diagnosticRun = ref<string>();
  const diagnosticWindow = reactive({ from: '', until: '' });
  const logFilters = reactive({ component: '', level: '', keyword: '' });
  const logs = ref<{
    status: string;
    requestIdSuffix?: string;
    nextCursor?: string;
    from?: number | string;
    until?: number | string;
    rows?: { timestamp: number | string; component?: string; level?: string; message: string }[];
  }>();
  const metrics = ref<{
    status: string;
    source?: string;
    from?: number | string;
    until?: number | string;
    series?: {
      name: string;
      unit: string;
      points: { timestamp: number | string; value: number | string }[];
    }[];
  }>();
  const snapshots = ref<Snapshot[]>([]);
  const operations = ref<Operation[]>([]);
  const selected = ref<Operation>();
  let selectionGeneration = 0;
  const restore = ref<
    'FROM_NEW' | 'FROM_LATEST' | 'FROM_CLOUD_LATEST' | 'FROM_CLOUD_SNAPSHOT' | 'FROM_SNAPSHOT'
  >('FROM_NEW');
  const snapshotId = ref<string>();
  const cloudSnapshotId = ref<string>();
  const fresh = ref(false);
  const submitInstance = ref(false);
  const sourceReleaseId = ref<string>();
  const pendingActions = ref(0);
  const busy = computed(() => pendingActions.value > 0);
  const error = ref('');
  const feedback = ref('');
  let timer: ReturnType<typeof setInterval> | undefined;
  let pendingRefresh: Promise<void> | undefined;
  const run = computed(() => runs.value.find((value) => value.id === job.value?.currentRunId));
  watch([restore, () => run.value?.id, () => job.value?.activeReleaseId], () => {
    fresh.value = false;
  });
  const running = computed(() => run.value?.run_state === 'RUNNING');
  const cloudVerificationRequired = computed(
    () =>
      (job.value?.cloudBound ?? Boolean(job.value?.activeReleaseId)) &&
      job.value?.syncStatus !== 'FRESH',
  );
  const lock = computed(
    () => busy.value || Boolean(job.value?.pendingOperationId) || cloudVerificationRequired.value,
  );
  const dirty = computed(
    () =>
      Boolean(spec.value) &&
      (JSON.stringify(draftContents()) !== savedSpec.value ||
        configurationText.value !== savedConfigurationText.value),
  );
  const can = (key: string) => capabilities.value[key] === 'AVAILABLE';
  const canRun = (key: string) => runtimeCapabilities.value[key] === 'AVAILABLE';
  const showVolcengineLogConsole = computed(
    () =>
      job.value?.provider === 'volcengine' &&
      hasPermission('app:log') &&
      (!canRun('LOG_SEARCH') ||
        ['NEEDS_CONFIGURATION', 'NO_ACCESS', 'EXPIRED', 'STALE'].includes(
          logs.value?.status || '',
        )),
  );
  const publishCapability = computed(() =>
    spec.value?.type === 'SQL' && (spec.value.udfArtifactId || spec.value.dependencyIds?.length)
      ? 'PUBLISH_SQL_UDF'
      : 'PUBLISH_' + spec.value?.type,
  );
  const restoreChoices = computed(() =>
    snapshots.value.filter(
      (value) => value.restorable && value.release_id === job.value?.activeReleaseId,
    ),
  );

  function refresh(initial = false): Promise<void> {
    if (pendingRefresh) {
      return initial ? pendingRefresh.catch(() => {}).then(() => refresh(true)) : pendingRefresh;
    }
    pendingRefresh = load(initial).finally(() => {
      pendingRefresh = undefined;
    });
    return pendingRefresh;
  }

  async function load(initial: boolean) {
    const [info, rel, allRuns, points, ops] = await Promise.all([
      managedGet<Job>(base),
      managedGet<Release[]>(base + '/releases'),
      managedGet<Run[]>(base + '/runs'),
      managedGet<Snapshot[]>(base + '/snapshots'),
      managedGet<Operation[]>(base + '/operations'),
    ]);
    job.value = info;
    releases.value = rel;
    runs.value = allRuns;
    snapshots.value = points;
    operations.value = ops;
    if (initial || !spec.value) {
      spec.value = info.spec;
      argText.value = info.spec.args.join('\n');
      configurationText.value = Object.entries(info.spec.configuration)
        .map(([key, value]) => `${key}=${value}`)
        .join('\n');
      savedConfigurationText.value = configurationText.value;
      editingRevision.value = info.draftRevision;
      savedSpec.value = JSON.stringify(draftContents());
    }
    if (selected.value) {
      await readSelection(selected.value.id, selectionGeneration);
    }
  }

  async function action(task: () => Promise<void>) {
    pendingActions.value++;
    error.value = '';
    feedback.value = '';
    try {
      await task();
    } catch (cause) {
      error.value = (cause as Error).message || t('managedFlink.failed');
    } finally {
      pendingActions.value--;
    }
  }

  function formatSql() {
    if (spec.value?.sql) spec.value.sql = format(spec.value.sql);
  }

  function changeSchedule(policy: Spec['schedulePolicy']) {
    if (!spec.value) return;
    spec.value.schedulePolicy = policy;
    if (policy === 'DRF') delete spec.value.scheduleTimeout;
  }

  function draftContents() {
    if (!spec.value) throw new Error(t('managedFlink.failed'));
    return { ...spec.value, args: argText.value ? argText.value.split('\n') : [] };
  }

  function draftSpec() {
    const configuration = new Map<string, string>();
    for (const line of configurationText.value.split('\n')) {
      if (!line.trim()) continue;
      const equals = line.indexOf('=');
      const key = line.slice(0, equals).trim();
      if (equals < 1 || !key || configuration.has(key)) {
        throw new Error(t('managedFlink.invalidConfiguration'));
      }
      configuration.set(key, line.slice(equals + 1));
    }
    return { ...draftContents(), configuration: Object.fromEntries(configuration) };
  }

  function save() {
    void action(async () => {
      await managedPatch(base, {
        expectedDraftRevision: editingRevision.value,
        spec: draftSpec(),
      });
      await refresh(true);
      feedback.value = t('managedFlink.saved');
    });
  }

  function validate() {
    void action(async () => {
      await managedPost(base + '/validate', draftSpec());
      feedback.value = t('managedFlink.validated');
    });
  }

  function validateCloudSql() {
    void action(async () => {
      const result = await managedPost<{ status: string }>(base + '/validate-cloud-sql', {
        sql: spec.value?.sql,
      });
      feedback.value = t('managedFlink.cloudSqlResult.' + result.status);
    });
  }

  function removeJar(id: string) {
    if (!spec.value) return;
    if (spec.value.udfArtifactId === id) delete spec.value.udfArtifactId;
    else spec.value.dependencyIds = spec.value.dependencyIds?.filter((value) => value !== id);
  }

  function uploadDependencies(event: Event) {
    const input = event.target as HTMLInputElement;
    const files = Array.from(input.files || []);
    void action(async () => {
      for (const file of files) {
        const id = await uploadManagedJar(props.appId, file);
        if (!spec.value) return;
        const current = spec.value.dependencyIds || [];
        if (!current.includes(id)) spec.value.dependencyIds = [...current, id];
      }
      input.value = '';
    });
  }

  function upload(event: Event) {
    const input = event.target as HTMLInputElement;
    const file = input.files?.[0];
    if (!file || !spec.value) return;
    void action(async () => {
      const artifactId = await uploadManagedJar(props.appId, file);
      await attachArtifact(artifactId);
      input.value = '';
    });
  }

  async function attachArtifact(artifactId: string) {
    const artifact = await managedGet<{ main_class?: string; digest: string }>(
      base + '/artifacts/' + artifactId,
    );
    if (spec.value?.type === 'SQL') {
      spec.value.udfArtifactId = artifactId;
    } else if (spec.value) {
      spec.value.artifactId = artifactId;
      if (!spec.value.mainClass) spec.value.mainClass = artifact.main_class;
    }
    feedback.value = artifact.digest;
  }

  function importBuild() {
    void action(async () => {
      const artifactId = await managedPost<string>(base + '/artifacts/project', { ...buildSource });
      await attachArtifact(artifactId);
    });
  }

  function command(name: string, data: Record<string, unknown>) {
    if (lock.value) return;
    void action(async () => {
      const fingerprint = 'managed-flink-command:' + JSON.stringify([props.appId, name, data]);
      const key = sessionStorage.getItem(fingerprint) || buildUUID();
      sessionStorage.setItem(fingerprint, key);
      const accepted = await managedPost<{ operationId: string }>(base + '/' + name, data, key);
      sessionStorage.removeItem(fingerprint);
      fresh.value = false;
      selected.value = {
        id: accepted.operationId,
        command_type: name.toUpperCase(),
        status: 'QUEUED',
      };
      feedback.value = accepted.operationId;
      await readSelection(accepted.operationId, ++selectionGeneration);
      await refresh();
    });
  }

  type Publication = 'publish' | 'rollback' | 'submit';

  function publicationRequest(name: Publication) {
    return {
      draftRevision: editingRevision.value,
      expectedReleaseId: job.value?.activeReleaseId,
      expectedEnvVersion: job.value?.configVersion,
      restore: name === 'submit' ? { mode: 'FROM_NEW', confirmedNew: true } : undefined,
      sourceReleaseId: name === 'rollback' ? sourceReleaseId.value : undefined,
    };
  }

  function publicationDisabled(name: Publication) {
    return (
      lock.value ||
      running.value ||
      !can('PUBLISH_' + spec.value?.type) ||
      (name === 'rollback' ? !sourceReleaseId.value : dirty.value || !can(publishCapability.value))
    );
  }

  function publish(name: Publication) {
    if (!publicationDisabled(name)) command(name, publicationRequest(name));
  }

  async function readSelection(id: string, generation: number) {
    const detail = await managedGet<Operation>(base + '/operations/' + id);
    if (generation === selectionGeneration) selected.value = detail;
  }

  function details(id: string) {
    const generation = ++selectionGeneration;
    selected.value = undefined;
    void action(() => readSelection(id, generation));
  }

  function closeDetails() {
    selectionGeneration++;
    selected.value = undefined;
  }

  function reconcile(id: string) {
    void action(async () => {
      await managedPost(base + '/operations/' + id + '/reconcile', {});
      await refresh();
    });
  }

  function clearDiagnostics() {
    logs.value = undefined;
    metrics.value = undefined;
  }

  function diagnosticParams() {
    return {
      from: diagnosticWindow.from ? new Date(diagnosticWindow.from).getTime() : undefined,
      until: diagnosticWindow.until ? new Date(diagnosticWindow.until).getTime() : undefined,
    };
  }

  function readMetrics() {
    const id = diagnosticRun.value || job.value?.currentRunId;
    if (!id || busy.value) return;
    diagnosticRun.value = id;
    void action(async () => {
      metrics.value = await managedGet(base + `/runs/${id}/metrics`, diagnosticParams());
    });
  }

  function readLogs(cursor?: string) {
    const id = diagnosticRun.value || job.value?.currentRunId;
    if (!id || busy.value) return;
    diagnosticRun.value = id;
    void action(async () => {
      logs.value = await managedGet(base + `/runs/${id}/logs`, {
        component: logFilters.component || undefined,
        level: logFilters.level || undefined,
        keyword: logFilters.keyword || undefined,
        ...diagnosticParams(),
        cursor,
      });
    });
  }

  onMounted(() => {
    void action(() => refresh(true));
    timer = setInterval(() => {
      if (!document.hidden && !busy.value) void refresh().catch(() => {});
    }, 3000);
  });

  onUnmounted(() => {
    if (timer) clearInterval(timer);
  });
</script>

<template>
  <section class="p-6">
    <h2>{{ name || t('managedFlink.title') }}</h2>
    <Alert v-if="error" class="mb-4" :message="error" type="error" show-icon />
    <Alert v-if="feedback" class="mb-4" :message="feedback" show-icon />
    <template v-if="job && spec">
      <Space class="mb-4" wrap>
        <Tag>{{ job.environmentName }}</Tag>
        <Tag>{{ job.engineRef }}</Tag>
        <Tag>{{ t(spec.batchMode ? 'managedFlink.batch' : 'managedFlink.streaming') }}</Tag>
        <Tag>{{ job.syncStatus }}</Tag>
        <Tag>{{ run?.run_state || 'NOT_STARTED' }}</Tag>
        <span v-if="job.observedAt && Number(job.observedAt) > 0">
          {{ t('managedFlink.lastCloudRead') }}
          {{ new Date(Number(job.observedAt)).toLocaleString() }}
        </span>
      </Space>
      <Alert
        v-if="cloudVerificationRequired"
        class="mb-4"
        type="warning"
        :message="t('managedFlink.cloudVerificationRequired')"
        show-icon
      />
      <Tabs>
        <TabPane key="draft" :tab="t('managedFlink.draft')">
          <Form layout="vertical">
            <FormItem v-if="spec.type === 'SQL'" label="SQL">
              <Textarea :disabled="busy" v-model:value="spec.sql" :rows="10" />
              <Button size="small" :disabled="busy || !spec.sql" @click="formatSql">
                {{ t('flink.app.flinkSql.format') }}
              </Button>
            </FormItem>
            <FormItem :label="spec.type === 'SQL' ? 'UDF JAR' : 'JAR'">
              <input type="file" accept=".jar" :disabled="busy" @change="upload" />
              <div v-if="spec.artifactId">{{ spec.artifactId }}</div>
              <Space v-if="spec.udfArtifactId">
                {{ spec.udfArtifactId }}
                <Button size="small" :disabled="busy" @click="removeJar(spec?.udfArtifactId || '')">
                  {{ t('common.delText') }}
                </Button>
              </Space>
            </FormItem>
            <FormItem :label="t('managedFlink.dependencies')">
              <input
                type="file"
                multiple
                accept=".jar"
                :disabled="busy"
                @change="uploadDependencies"
              />
              <div v-for="id in spec.dependencyIds" :key="id">
                {{ id }}
                <Button size="small" :disabled="busy" @click="removeJar(id)">
                  {{ t('common.delText') }}
                </Button>
              </div>
            </FormItem>
            <FormItem :label="t('managedFlink.importBuild')" v-auth="'project:view'">
              <Space wrap>
                <Input
                  :disabled="busy"
                  v-model:value="buildSource.projectId"
                  :placeholder="t('managedFlink.projectId')"
                />
                <Input
                  :disabled="busy"
                  v-model:value="buildSource.module"
                  :placeholder="t('managedFlink.module')"
                />
                <Input
                  :disabled="busy"
                  v-model:value="buildSource.jar"
                  placeholder="application.jar"
                />
                <Button
                  :disabled="
                    busy || !buildSource.projectId || !buildSource.module || !buildSource.jar
                  "
                  @click="importBuild"
                  >{{ t('managedFlink.importBuild') }}</Button
                >
              </Space>
            </FormItem>
            <template v-if="spec.type === 'JAR'">
              <FormItem :label="t('managedFlink.mainClass')"
                ><Input :disabled="busy" v-model:value="spec.mainClass"
              /></FormItem>
              <FormItem :label="t('managedFlink.args')"
                ><Textarea :disabled="busy" v-model:value="argText" :rows="4"
              /></FormItem>
            </template>
            <template v-if="job.provider === 'volcengine'">
              <FormItem :label="t('managedFlink.priority')">
                <InputNumber
                  :disabled="busy"
                  v-model:value="spec.priority"
                  :min="0"
                  :precision="0"
                  placeholder="3"
                />
              </FormItem>
              <FormItem :label="t('managedFlink.schedulePolicy')">
                <Select
                  :disabled="busy"
                  :value="spec.schedulePolicy || (spec.batchMode ? 'DRF' : 'GANG')"
                  :options="
                    (spec.batchMode ? ['DRF'] : ['GANG', 'DRF']).map((value) => ({ value }))
                  "
                  @change="changeSchedule"
                />
              </FormItem>
              <FormItem
                v-if="!spec.batchMode && spec.schedulePolicy !== 'DRF'"
                :label="t('managedFlink.scheduleTimeout')"
              >
                <InputNumber
                  :disabled="busy"
                  v-model:value="spec.scheduleTimeout"
                  :min="0"
                  :precision="0"
                  placeholder="60"
                />
              </FormItem>
            </template>
            <FormItem :label="t('managedFlink.configuration')">
              <Textarea
                :disabled="busy"
                v-model:value="configurationText"
                :rows="4"
                :placeholder="
                  spec.type === 'SQL'
                    ? 'pipeline.global-job-parameters=label:example,region:beijing'
                    : 'execution.checkpointing.interval=4 s'
                "
              />
            </FormItem>
            <Space wrap>
              <Button v-auth="'app:update'" :disabled="busy" @click="save">{{
                t('managedFlink.save')
              }}</Button>
              <Button v-auth="'app:update'" :disabled="busy" @click="validate">{{
                t('managedFlink.validate')
              }}</Button>
              <Button
                v-if="job.provider === 'volcengine' && spec.type === 'SQL'"
                v-auth="'app:update'"
                :disabled="busy || !spec.sql"
                @click="validateCloudSql"
                >{{ t('managedFlink.validateCloudSql') }}</Button
              >
              <Popconfirm
                :key="JSON.stringify(publicationRequest('submit'))"
                v-if="
                  can('SUBMIT_INSTANCE') &&
                  (!job.cloudBound || job.directSubmission) &&
                  hasPermission('app:start') &&
                  hasPermission('app:create') &&
                  hasPermission('app:release')
                "
                :title="t('managedFlink.directConfirm')"
                :disabled="publicationDisabled('submit')"
                @confirm="publish('submit')"
              >
                <Button :disabled="publicationDisabled('submit')">{{
                  t('managedFlink.directSubmit')
                }}</Button>
              </Popconfirm>
              <Popconfirm
                :key="JSON.stringify(publicationRequest('publish'))"
                v-if="!job.directSubmission"
                :title="t('managedFlink.publishConfirm')"
                :disabled="publicationDisabled('publish')"
                @confirm="publish('publish')"
              >
                <Button
                  v-auth="'app:release'"
                  type="primary"
                  :disabled="publicationDisabled('publish')"
                >
                  {{ t('managedFlink.publish') }}
                </Button>
              </Popconfirm>
              <span v-if="!can(publishCapability)">{{
                capabilities[publishCapability] || 'UNVERIFIED'
              }}</span>
            </Space>
          </Form>
        </TabPane>
        <TabPane key="run" :tab="t('managedFlink.runs')">
          <Space class="mb-4" wrap>
            <Select
              v-model:value="restore"
              :options="[
                { value: 'FROM_NEW', label: t('managedFlink.restartNew') },
                {
                  value: 'FROM_LATEST',
                  label: t('managedFlink.latest'),
                  disabled: !canRun('RESTORE_FROM_LATEST'),
                },
                {
                  value: 'FROM_CLOUD_LATEST',
                  label: t('managedFlink.cloudLatest'),
                  disabled: !canRun('RESTORE_FROM_CLOUD_LATEST'),
                },
                {
                  value: 'FROM_CLOUD_SNAPSHOT',
                  label: t('managedFlink.cloudSnapshot'),
                  disabled: !canRun('RESTORE_FROM_CLOUD_SNAPSHOT'),
                },
                {
                  value: 'FROM_SNAPSHOT',
                  label: t('managedFlink.snapshot'),
                  disabled: !canRun('RESTORE_FROM_SNAPSHOT'),
                },
              ]"
            />
            <template v-if="restore === 'FROM_CLOUD_SNAPSHOT'">
              <Button :loading="busy" @click="readCloud()">{{
                t('managedFlink.readCloud')
              }}</Button>
              <Select
                v-model:value="cloudSnapshotId"
                style="min-width: 240px"
                :options="
                  (cloud?.savepoints || [])
                    .filter((p) => p.Status === 'AVAILABLE')
                    .map((p) => ({ value: p.SavepointId, label: p.SavepointId }))
                "
              />
            </template>
            <Checkbox
              v-if="canRun('SUBMIT_INSTANCE') && !job.directSubmission"
              v-model:checked="submitInstance"
              >{{ t('managedFlink.submitInstance') }}</Checkbox
            >
            <Checkbox v-if="restore === 'FROM_NEW' && run" v-model:checked="fresh">{{
              t('managedFlink.confirmNew')
            }}</Checkbox>
            <Select
              v-if="restore === 'FROM_SNAPSHOT'"
              v-model:value="snapshotId"
              style="min-width: 240px"
              :options="restoreChoices.map(({ id }) => ({ value: id }))"
            />
            <Button
              v-auth="'app:start'"
              :disabled="lock || running || !job.activeReleaseId || !canRun('START')"
              @click="
                command('start', {
                  expectedReleaseId: job?.activeReleaseId,
                  submitInstance,
                  restore: {
                    mode: restore,
                    snapshotId: restore === 'FROM_CLOUD_SNAPSHOT' ? cloudSnapshotId : snapshotId,
                    confirmedNew: fresh,
                  },
                })
              "
            >
              {{ t('managedFlink.start') }}
            </Button>
            <Popconfirm
              :key="
                JSON.stringify([
                  run?.id,
                  job?.activeReleaseId,
                  submitInstance,
                  restore,
                  snapshotId,
                  cloudSnapshotId,
                  fresh,
                ])
              "
              v-if="running && hasPermission('app:cancel') && hasPermission('app:start')"
              :title="t('managedFlink.restart')"
              @confirm="
                command('restart', {
                  expectedRunId: run?.id,
                  expectedReleaseId: job?.activeReleaseId,
                  submitInstance,
                  restore: {
                    mode: restore,
                    snapshotId: restore === 'FROM_CLOUD_SNAPSHOT' ? cloudSnapshotId : snapshotId,
                    confirmedNew: fresh,
                  },
                })
              "
            >
              <Button :disabled="lock || !canRun('START') || !canRun('STOP')">{{
                t('managedFlink.restart')
              }}</Button>
            </Popconfirm>
            <Popconfirm
              :key="'stop-' + run?.id"
              :title="t('managedFlink.stop')"
              :disabled="lock"
              @confirm="command('stop', { expectedRunId: run?.id, mode: 'IMMEDIATE' })"
            >
              <Button v-auth="'app:cancel'" :disabled="lock || !running || !canRun('STOP')">{{
                t('managedFlink.stop')
              }}</Button>
            </Popconfirm>
            <Popconfirm
              :key="'snapshotStopConfirm-' + run?.id"
              :title="t('managedFlink.snapshotStopConfirm')"
              :disabled="lock"
              @confirm="command('stop', { expectedRunId: run?.id, mode: 'AFTER_SNAPSHOT' })"
            >
              <Button
                v-auth="'app:cancel'"
                :disabled="lock || !running || !canRun('SNAPSHOT_THEN_STOP')"
                >{{ t('managedFlink.snapshotStop') }}</Button
              >
            </Popconfirm>
            <Button
              v-auth="'savepoint:trigger'"
              :disabled="lock || !running || !canRun('SNAPSHOT')"
              @click="command('snapshots', { expectedRunId: run?.id })"
              >{{ t('managedFlink.snapshot') }}</Button
            >
          </Space>
          <List :data-source="runs">
            <template #renderItem="{ item }"
              ><ListItem
                >{{ item.provider_instance_id }} · {{ item.run_state }} ·
                {{ item.origin }}</ListItem
              ></template
            >
          </List>
        </TabPane>
        <TabPane key="release" :tab="t('managedFlink.versions')">
          <Space v-if="!job.directSubmission" class="mb-4">
            <Select
              v-model:value="sourceReleaseId"
              style="min-width: 180px"
              :options="
                releases
                  .filter((item) => item.status === 'PUBLISHED')
                  .map((item) => ({ value: item.id, label: 'R' + item.release_no }))
              "
            />
            <Popconfirm
              :key="JSON.stringify(publicationRequest('rollback'))"
              :title="t('managedFlink.publishConfirm')"
              :disabled="publicationDisabled('rollback')"
              @confirm="publish('rollback')"
            >
              <Button
                v-auth="'app:release'"
                :disabled="publicationDisabled('rollback')"
              >
                {{ t('managedFlink.rollback') }}
              </Button>
            </Popconfirm>
          </Space>
          <List :data-source="releases">
            <template #renderItem="{ item }"
              ><ListItem
                >R{{ item.release_no }} · {{ item.status }} · {{ item.provider_version }}</ListItem
              ></template
            >
          </List>
        </TabPane>
        <TabPane key="snapshot" :tab="t('managedFlink.snapshots')">
          <List :data-source="snapshots">
            <template #renderItem="{ item }"
              ><ListItem>{{ item.id }} · {{ item.state }}</ListItem></template
            >
          </List>
        </TabPane>
        <TabPane key="operations" :tab="t('managedFlink.operations')">
          <List :data-source="operations">
            <template #renderItem="{ item }">
              <ListItem>
                <Button type="link" @click="details(item.id)"
                  >{{ item.command_type }} · {{ item.status }}</Button
                >
              </ListItem>
            </template>
          </List>
        </TabPane>
        <TabPane v-if="job.provider === 'volcengine'" key="cloud" :tab="t('managedFlink.cloudJob')">
          <Button :loading="busy" :disabled="!job.cloudBound" @click="readCloud()">{{
            t('managedFlink.readCloud')
          }}</Button>
          <Alert
            v-if="cloud && cloud.status !== 'AVAILABLE'"
            :message="cloud.status"
            type="warning"
          />
          <template v-if="cloud?.status === 'AVAILABLE'">
            <p>{{ cloud.jobId }} · {{ cloud.jobType }}</p>
            <p>{{ t('managedFlink.startMethods') }} · {{ cloud.startMethods?.join(', ') }}</p>
            <List :header="t('managedFlink.runs')" :data-source="cloud.instances">
              <template #renderItem="{ item }">
                <ListItem>
                  <Button type="link" :disabled="busy" @click="readCloudInstance(item.Id)">{{
                    item.Id
                  }}</Button>
                  {{ item.State }} · {{ item.StartTime }}
                </ListItem>
              </template>
            </List>
            <p v-if="cloudInstance">
              {{ cloudInstance.id }} · {{ cloudInstance.state || cloudInstance.status }} ·
              {{ cloudInstance.engine }} {{ cloudInstance.runtimeId }}
              <a
                v-if="cloudInstance.dashboard"
                :href="cloudInstance.dashboard"
                target="_blank"
                rel="noopener noreferrer"
                >Flink UI</a
              >
            </p>
            <Space>
              <Button
                :disabled="!cloud.page || cloud.page === 1"
                @click="readCloud((cloud?.page || 1) - 1)"
                >{{ t('managedFlink.prevPage') }}</Button
              >
              <Button :disabled="!cloud.hasNext" @click="readCloud((cloud?.page || 1) + 1)">{{
                t('managedFlink.nextPage')
              }}</Button>
            </Space>
            <List :header="t('managedFlink.snapshots')" :data-source="cloud.savepoints">
              <template #renderItem="{ item }"
                ><ListItem
                  >{{ item.SavepointId }} · {{ item.Status }} · {{ item.CreateTime }}</ListItem
                ></template
              >
            </List>
          </template>
        </TabPane>
        <TabPane key="diagnostics" :tab="t('managedFlink.diagnostics')">
          <Space
            v-if="
              runs.length &&
              (canRun('METRICS') || (canRun('LOG_SEARCH') && hasPermission('app:log')))
            "
            class="mb-4"
            wrap
          >
            <Select
              :value="diagnosticRun || job.currentRunId"
              :aria-label="t('managedFlink.diagnosticInstance')"
              :disabled="busy"
              :placeholder="t('managedFlink.diagnosticInstance')"
              style="min-width: 220px"
              :options="
                runs.map((entry) => ({
                  value: entry.id,
                  label: `${entry.provider_instance_id} · ${entry.run_state}`,
                }))
              "
              @change="
                (id) => {
                  diagnosticRun = String(id);
                  clearDiagnostics();
                }
              "
            />
            <label>
              {{ t('managedFlink.diagnosticFrom') }}
              <Input
                v-model:value="diagnosticWindow.from"
                type="datetime-local"
                :disabled="busy"
                @input="clearDiagnostics"
              />
            </label>
            <label>
              {{ t('managedFlink.diagnosticUntil') }}
              <Input
                v-model:value="diagnosticWindow.until"
                type="datetime-local"
                :disabled="busy"
                @input="clearDiagnostics"
              />
            </label>
          </Space>
          <p>
            {{ t('managedFlink.logs') }} · {{ runtimeCapabilities.LOG_SEARCH || 'UNVERIFIED' }}
            <a
              v-if="showVolcengineLogConsole"
              class="ml-2"
              href="https://console.volcengine.com/flink"
              target="_blank"
              rel="noopener noreferrer"
              >{{ t('managedFlink.openVolcengineConsole') }}</a
            >
          </p>
          <div v-if="canRun('LOG_SEARCH') && runs.length && hasPermission('app:log')" class="mb-6">
            <Space wrap>
              <Input
                v-model:value="logFilters.component"
                :aria-label="t('managedFlink.logComponent')"
                :placeholder="t('managedFlink.logComponent')"
                :disabled="busy"
                :maxlength="64"
                @input="logs = undefined"
              />
              <Input
                v-if="canRun('LOG_LEVEL_FILTER')"
                v-model:value="logFilters.level"
                :aria-label="t('managedFlink.logLevel')"
                :placeholder="t('managedFlink.logLevel')"
                :disabled="busy"
                :maxlength="16"
                @input="logs = undefined"
              />
              <Input
                v-model:value="logFilters.keyword"
                :aria-label="t('managedFlink.logKeyword')"
                :placeholder="t('managedFlink.logKeyword')"
                :disabled="busy"
                :maxlength="256"
                @input="logs = undefined"
              />
              <Button
                :loading="busy"
                :disabled="!(diagnosticRun || job.currentRunId)"
                @click="readLogs()"
              >
                {{ t('managedFlink.loadLogs') }}
              </Button>
            </Space>
            <div v-if="logs" class="mt-4">
              <p>
                {{ t('managedFlink.logStatus.' + logs.status) }}
                <span v-if="logs.requestIdSuffix"> · RequestId …{{ logs.requestIdSuffix }}</span>
              </p>
              <p v-if="logs.from !== undefined && logs.until !== undefined">
                {{ new Date(Number(logs.from)).toLocaleString() }} →
                {{ new Date(Number(logs.until)).toLocaleString() }}
              </p>
              <ul>
                <li v-for="(entry, index) in logs.rows" :key="index" class="mb-2">
                  {{ new Date(Number(entry.timestamp)).toLocaleString() }} {{ entry.component }}
                  {{ entry.level }}
                  <pre class="whitespace-pre-wrap break-words">{{ entry.message }}</pre>
                </li>
              </ul>
              <Button v-if="logs.nextCursor" :loading="busy" @click="readLogs(logs?.nextCursor)">
                {{ t('managedFlink.nextLogPage') }}
              </Button>
            </div>
          </div>
          <p>{{ t('managedFlink.metrics') }} · {{ runtimeCapabilities.METRICS || 'UNVERIFIED' }}</p>
          <template v-if="canRun('METRICS') && runs.length">
            <Button
              :loading="busy"
              :disabled="!(diagnosticRun || job.currentRunId)"
              @click="readMetrics"
              >{{ t('managedFlink.loadMetrics') }}</Button
            >
            <div v-if="metrics" class="mt-4">
              <p>{{ metrics.source }} {{ metrics.status }}</p>
              <p v-if="metrics.from !== undefined && metrics.until !== undefined">
                {{ new Date(Number(metrics.from) * 1000).toLocaleString() }} →
                {{ new Date(Number(metrics.until) * 1000).toLocaleString() }}
              </p>
              <details v-for="series in metrics.series" :key="series.name" class="mb-2">
                <summary>{{ series.name }} · {{ series.unit }}</summary>
                <ul>
                  <li v-for="point in series.points" :key="point.timestamp">
                    {{ new Date(Number(point.timestamp) * 1000).toLocaleString() }} ·
                    {{ point.value }}
                  </li>
                </ul>
              </details>
            </div>
          </template>
        </TabPane>
      </Tabs>
      <Drawer :visible="Boolean(selected)" :title="selected?.id" width="480" @close="closeDetails">
        <p>{{ selected?.command_type }} · {{ selected?.status }}</p>
        <List :data-source="selected?.steps || []">
          <template #renderItem="{ item }">
            <ListItem
              >{{ item.action }} · {{ item.state }} · {{ item.error_code }} ·
              {{ item.request_id }}</ListItem
            >
          </template>
        </List>
        <Button
          v-if="selected?.status === 'VERIFY_REQUIRED'"
          v-auth="'app:release'"
          :loading="busy"
          @click="selected && reconcile(selected.id)"
          >{{ t('managedFlink.reconcile') }}</Button
        >
      </Drawer>
    </template>
  </section>
</template>
