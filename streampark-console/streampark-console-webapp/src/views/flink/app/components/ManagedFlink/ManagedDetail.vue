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
    List,
    Popconfirm,
    Select,
    Space,
    Tabs,
    Tag,
  } from 'ant-design-vue';
  import { computed, onMounted, onUnmounted, reactive, ref } from 'vue';
  import { useI18n } from '/@/hooks/web/useI18n';
  import { usePermission } from '/@/hooks/web/usePermission';
  import {
    managedGet,
    managedPatch,
    managedPost,
    uploadManagedJar,
    type Job,
    type Spec,
    type Environment,
    type Capabilities,
    type Operation,
    type Release,
    type Run,
    type Snapshot,
  } from '/@/api/flink/managedFlink';
  import { buildUUID } from '/@/utils/uuid';

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
  const environment = ref<Environment>();
  const capabilities = ref<Capabilities>({});
  const releases = ref<Release[]>([]);
  const runs = ref<Run[]>([]);
  const logRun = ref<string>();
  const logFilters = reactive({ component: '', level: '', keyword: '', from: '', until: '' });
  const logs = ref<{
    status: string;
    requestIdSuffix?: string;
    nextCursor?: string;
    from?: number;
    until?: number;
    rows?: { timestamp: number; component?: string; level?: string; message: string }[];
  }>();
  const metricRun = ref<string>();
  const metrics = ref<{
    status: string;
    source?: string;
    series?: { name: string; unit: string; points: { timestamp: number; value: number }[] }[];
  }>();
  const snapshots = ref<Snapshot[]>([]);
  const operations = ref<Operation[]>([]);
  const selected = ref<Operation>();
  const restore = ref<'FROM_NEW' | 'FROM_LATEST' | 'FROM_SNAPSHOT'>('FROM_NEW');
  const snapshotId = ref<string>();
  const fresh = ref(false);
  const sourceReleaseId = ref<string>();
  const busy = ref(false);
  const error = ref('');
  const feedback = ref('');
  let timer: ReturnType<typeof setInterval> | undefined;
  let pendingRefresh: Promise<void> | undefined;
  const run = computed(() => runs.value.find((value) => value.id === job.value?.currentRunId));
  const running = computed(() => run.value?.run_state === 'RUNNING');
  const lock = computed(() => busy.value || Boolean(job.value?.pendingOperationId));
  const dirty = computed(
    () =>
      Boolean(spec.value) &&
      (JSON.stringify(draftContents()) !== savedSpec.value ||
        configurationText.value !== savedConfigurationText.value),
  );
  const can = (key: string) => capabilities.value[key] === 'AVAILABLE';
  const publishCapability = computed(() =>
    spec.value?.type === 'SQL' && spec.value.udfArtifactId
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
    if (initial) {
      const envs = await managedGet<Environment[]>('/environments');
      environment.value = envs.find((value) => value.id === info.environmentId);
      capabilities.value = await managedGet<Capabilities>(
        '/environments/' + info.environmentId + '/capabilities',
      );
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
      selected.value = await managedGet<Operation>(base + '/operations/' + selected.value.id);
    }
  }

  async function action(task: () => Promise<void>) {
    busy.value = true;
    error.value = '';
    feedback.value = '';
    try {
      await task();
    } catch (cause) {
      error.value = (cause as Error).message || t('managedFlink.failed');
    } finally {
      busy.value = false;
    }
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
    void action(async () => {
      const fingerprint = 'managed-flink-command:' + JSON.stringify([props.appId, name, data]);
      const key = sessionStorage.getItem(fingerprint) || buildUUID();
      sessionStorage.setItem(fingerprint, key);
      const accepted = await managedPost<{ operationId: string }>(base + '/' + name, data, key);
      sessionStorage.removeItem(fingerprint);
      selected.value = await managedGet(base + '/operations/' + accepted.operationId);
      await refresh();
    });
  }

  function publish(name: 'publish' | 'rollback') {
    command(name, {
      draftRevision: job.value?.draftRevision,
      expectedReleaseId: job.value?.activeReleaseId,
      expectedEnvVersion: environment.value?.configVersion,
      sourceReleaseId: name === 'rollback' ? sourceReleaseId.value : undefined,
    });
  }

  function details(id: string) {
    void action(async () => {
      selected.value = await managedGet(base + '/operations/' + id);
    });
  }

  function reconcile(id: string) {
    void action(async () => {
      await managedPost(base + '/operations/' + id + '/reconcile', {});
      await refresh();
    });
  }

  function readMetrics() {
    const id = metricRun.value || job.value?.currentRunId;
    if (!id) return;
    metricRun.value = id;
    void action(async () => {
      metrics.value = await managedGet(base + `/runs/${id}/metrics`);
    });
  }

  function readLogs(cursor?: string) {
    const id = logRun.value || job.value?.currentRunId;
    if (!id) return;
    logRun.value = id;
    const from = logFilters.from ? new Date(logFilters.from).getTime() : undefined;
    const until = logFilters.until ? new Date(logFilters.until).getTime() : undefined;
    void action(async () => {
      logs.value = await managedGet(base + `/runs/${id}/logs`, {
        component: logFilters.component || undefined,
        level: logFilters.level || undefined,
        keyword: logFilters.keyword || undefined,
        from,
        until,
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
        <Tag>{{ environment?.name }}</Tag>
        <Tag>{{ job.engineRef }}</Tag>
        <Tag>{{ job.syncStatus }}</Tag>
        <Tag>{{ run?.run_state || 'NOT_STARTED' }}</Tag>
      </Space>
      <Tabs>
        <TabPane key="draft" :tab="t('managedFlink.draft')">
          <Form layout="vertical">
            <FormItem v-if="spec.type === 'SQL'" label="SQL">
              <Textarea v-model:value="spec.sql" :rows="10" />
            </FormItem>
            <FormItem :label="spec.type === 'SQL' ? 'UDF JAR' : 'JAR'">
              <input type="file" accept=".jar" :disabled="busy" @change="upload" />
              <div v-if="spec.artifactId">{{ spec.artifactId }}</div>
              <Space v-if="spec.udfArtifactId">
                {{ spec.udfArtifactId }}
                <Button size="small" :disabled="busy" @click="spec.udfArtifactId = undefined">
                  {{ t('common.delText') }}
                </Button>
              </Space>
            </FormItem>
            <FormItem :label="t('managedFlink.importBuild')" v-auth="'project:view'">
              <Space wrap>
                <Input
                  v-model:value="buildSource.projectId"
                  :placeholder="t('managedFlink.projectId')"
                />
                <Input v-model:value="buildSource.module" :placeholder="t('managedFlink.module')" />
                <Input v-model:value="buildSource.jar" placeholder="application.jar" />
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
                ><Input v-model:value="spec.mainClass"
              /></FormItem>
              <FormItem :label="t('managedFlink.args')"
                ><Textarea v-model:value="argText" :rows="4"
              /></FormItem>
            </template>
            <FormItem :label="t('managedFlink.configuration')">
              <Textarea
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
              <Popconfirm :title="t('managedFlink.publishConfirm')" @confirm="publish('publish')">
                <Button
                  v-auth="'app:release'"
                  type="primary"
                  :disabled="
                    lock ||
                    dirty ||
                    running ||
                    !can('PUBLISH_' + spec.type) ||
                    !can(publishCapability)
                  "
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
                  disabled: !can('RESTORE_FROM_LATEST'),
                },
                {
                  value: 'FROM_SNAPSHOT',
                  label: t('managedFlink.snapshot'),
                  disabled: !can('RESTORE_FROM_SNAPSHOT'),
                },
              ]"
            />
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
              :disabled="lock || running || !job.activeReleaseId || !can('START')"
              @click="
                command('start', {
                  expectedReleaseId: job?.activeReleaseId,
                  restore: { mode: restore, snapshotId, confirmedNew: fresh },
                })
              "
            >
              {{ t('managedFlink.start') }}
            </Button>
            <Popconfirm
              :title="t('managedFlink.stop')"
              @confirm="command('stop', { expectedRunId: run?.id, mode: 'IMMEDIATE' })"
            >
              <Button v-auth="'app:cancel'" :disabled="lock || !running || !can('STOP')">{{
                t('managedFlink.stop')
              }}</Button>
            </Popconfirm>
            <Popconfirm
              :title="t('managedFlink.snapshotStopConfirm')"
              @confirm="command('stop', { expectedRunId: run?.id, mode: 'AFTER_SNAPSHOT' })"
            >
              <Button
                v-auth="'app:cancel'"
                :disabled="lock || !running || !can('SNAPSHOT_THEN_STOP')"
                >{{ t('managedFlink.snapshotStop') }}</Button
              >
            </Popconfirm>
            <Button
              v-auth="'savepoint:trigger'"
              :disabled="lock || !running || !can('SNAPSHOT')"
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
          <Space class="mb-4">
            <Select
              v-model:value="sourceReleaseId"
              style="min-width: 180px"
              :options="
                releases
                  .filter((item) => item.status === 'PUBLISHED')
                  .map((item) => ({ value: item.id, label: 'R' + item.release_no }))
              "
            />
            <Popconfirm :title="t('managedFlink.publishConfirm')" @confirm="publish('rollback')">
              <Button
                v-auth="'app:release'"
                :disabled="lock || running || !sourceReleaseId || !can('PUBLISH_' + spec.type)"
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
        <TabPane key="diagnostics" :tab="t('managedFlink.diagnostics')">
          <p>{{ t('managedFlink.logs') }} · {{ capabilities.LOG_SEARCH || 'UNVERIFIED' }}</p>
          <div v-if="can('LOG_SEARCH') && runs.length && hasPermission('app:log')" class="mb-6">
            <Space wrap>
              <Select
                :value="logRun || job.currentRunId"
                :aria-label="t('managedFlink.logInstance')"
                :disabled="busy"
                :placeholder="t('managedFlink.logInstance')"
                style="min-width: 220px"
                :options="
                  runs.map((entry) => ({
                    value: entry.id,
                    label: `${entry.provider_instance_id} · ${entry.run_state}`,
                  }))
                "
                @change="
                  (id) => {
                    logRun = String(id);
                    logs = undefined;
                  }
                "
              />
              <Input
                v-model:value="logFilters.component"
                :aria-label="t('managedFlink.logComponent')"
                :placeholder="t('managedFlink.logComponent')"
                :disabled="busy"
                :maxlength="64"
                @input="logs = undefined"
              />
              <Input
                v-if="can('LOG_LEVEL_FILTER')"
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
              <label
                >{{ t('managedFlink.logFrom') }}
                <Input
                  v-model:value="logFilters.from"
                  type="datetime-local"
                  :disabled="busy"
                  @input="logs = undefined"
                />
              </label>
              <label
                >{{ t('managedFlink.logUntil') }}
                <Input
                  v-model:value="logFilters.until"
                  type="datetime-local"
                  :disabled="busy"
                  @input="logs = undefined"
                />
              </label>
              <Button :loading="busy" :disabled="!(logRun || job.currentRunId)" @click="readLogs()">
                {{ t('managedFlink.loadLogs') }}
              </Button>
            </Space>
            <div v-if="logs" class="mt-4">
              <p>
                {{ t('managedFlink.logStatus.' + logs.status) }}
                <span v-if="logs.requestIdSuffix"> · RequestId …{{ logs.requestIdSuffix }}</span>
              </p>
              <p v-if="logs.from !== undefined && logs.until !== undefined">
                {{ new Date(logs.from).toLocaleString() }} →
                {{ new Date(logs.until).toLocaleString() }}
              </p>
              <ul>
                <li v-for="(entry, index) in logs.rows" :key="index" class="mb-2">
                  {{ new Date(entry.timestamp).toLocaleString() }} {{ entry.component }}
                  {{ entry.level }}
                  <pre class="whitespace-pre-wrap break-words">{{ entry.message }}</pre>
                </li>
              </ul>
              <Button v-if="logs.nextCursor" :loading="busy" @click="readLogs(logs.nextCursor)">
                {{ t('managedFlink.nextLogPage') }}
              </Button>
            </div>
          </div>
          <p>{{ t('managedFlink.metrics') }} · {{ capabilities.METRICS || 'UNVERIFIED' }}</p>
          <template v-if="can('METRICS') && runs.length">
            <Space wrap>
              <Select
                :value="metricRun || job.currentRunId"
                :disabled="busy"
                style="min-width: 220px"
                :options="
                  runs.map((entry) => ({
                    value: entry.id,
                    label: `${entry.provider_instance_id} · ${entry.run_state}`,
                  }))
                "
                @change="
                  (id) => {
                    metricRun = String(id);
                    metrics = undefined;
                  }
                "
              />
              <Button :loading="busy" @click="readMetrics">{{
                t('managedFlink.loadMetrics')
              }}</Button>
            </Space>
            <div v-if="metrics" class="mt-4">
              <p>{{ metrics.source }} {{ metrics.status }}</p>
              <details v-for="series in metrics.series" :key="series.name" class="mb-2">
                <summary>{{ series.name }} · {{ series.unit }}</summary>
                <ul>
                  <li v-for="point in series.points" :key="point.timestamp">
                    {{ new Date(point.timestamp * 1000).toLocaleString() }} · {{ point.value }}
                  </li>
                </ul>
              </details>
            </div>
          </template>
        </TabPane>
      </Tabs>
      <Drawer
        :visible="Boolean(selected)"
        :title="selected?.id"
        width="480"
        @close="selected = undefined"
      >
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
