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
  import { computed, onMounted, onUnmounted, reactive, ref } from 'vue';
  import { useI18n } from '/@/hooks/web/useI18n';
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

  const props = defineProps<{ appId: string; name?: string }>();
  const { t } = useI18n();
  const base = `/apps/${props.appId}`;
  const job = ref<Job>();
  const spec = ref<Spec>();
  const argText = ref('');
  const buildSource = reactive({ projectId: '', module: '', jar: '' });
  const editingRevision = ref<string>();
  const savedSpec = ref('');
  const environment = ref<Environment>();
  const capabilities = ref<Capabilities>({});
  const releases = ref<Release[]>([]);
  const runs = ref<Run[]>([]);
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
  let refreshing = false;
  const run = computed(() => runs.value.find((value) => value.id === job.value?.currentRunId));
  const running = computed(() => run.value?.run_state === 'RUNNING');
  const lock = computed(() => busy.value || Boolean(job.value?.pendingOperationId));
  const dirty = computed(
    () => Boolean(spec.value) && JSON.stringify(draftSpec()) !== savedSpec.value,
  );
  const can = (key: string) => capabilities.value[key] === 'AVAILABLE';
  const restoreChoices = computed(() =>
    snapshots.value.filter(
      (value) => value.restorable && value.release_id === job.value?.activeReleaseId,
    ),
  );

  async function refresh(initial = false) {
    if (refreshing) return;
    refreshing = true;
    try {
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
        editingRevision.value = info.draftRevision;
        savedSpec.value = JSON.stringify(draftSpec());
      }
      if (selected.value) {
        selected.value = await managedGet<Operation>(base + '/operations/' + selected.value.id);
      }
    } finally {
      refreshing = false;
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

  function draftSpec() {
    if (!spec.value) throw new Error(t('managedFlink.failed'));
    return { ...spec.value, args: argText.value ? argText.value.split('\n') : [] };
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
    if (spec.value) {
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
    <a-alert v-if="error" class="mb-4" :message="error" type="error" show-icon />
    <a-alert v-if="feedback" class="mb-4" :message="feedback" show-icon />
    <template v-if="job && spec">
      <a-space class="mb-4" wrap>
        <a-tag>{{ environment?.name }}</a-tag>
        <a-tag>{{ job.engineRef }}</a-tag>
        <a-tag>{{ job.syncStatus }}</a-tag>
        <a-tag>{{ run?.run_state || 'NOT_STARTED' }}</a-tag>
      </a-space>
      <a-tabs>
        <a-tab-pane key="draft" :tab="t('managedFlink.draft')">
          <a-form layout="vertical">
            <a-form-item v-if="spec.type === 'SQL'" label="SQL">
              <a-textarea v-model:value="spec.sql" :rows="10" />
            </a-form-item>
            <template v-else>
              <a-form-item label="JAR">
                <input type="file" accept=".jar" :disabled="busy" @change="upload" />
                <div v-if="spec.artifactId">{{ spec.artifactId }}</div>
              </a-form-item>
              <a-form-item :label="t('managedFlink.importBuild')" v-auth="'project:view'">
                <a-space wrap>
                  <a-input
                    v-model:value="buildSource.projectId"
                    :placeholder="t('managedFlink.projectId')"
                  />
                  <a-input
                    v-model:value="buildSource.module"
                    :placeholder="t('managedFlink.module')"
                  />
                  <a-input v-model:value="buildSource.jar" placeholder="application.jar" />
                  <a-button
                    :disabled="
                      busy || !buildSource.projectId || !buildSource.module || !buildSource.jar
                    "
                    @click="importBuild"
                    >{{ t('managedFlink.importBuild') }}</a-button
                  >
                </a-space>
              </a-form-item>
              <a-form-item :label="t('managedFlink.mainClass')"
                ><a-input v-model:value="spec.mainClass"
              /></a-form-item>
              <a-form-item :label="t('managedFlink.args')"
                ><a-textarea v-model:value="argText" :rows="4"
              /></a-form-item>
            </template>
            <a-space wrap>
              <a-button v-auth="'app:update'" :disabled="busy" @click="save">{{
                t('managedFlink.save')
              }}</a-button>
              <a-button v-auth="'app:update'" :disabled="busy" @click="validate">{{
                t('managedFlink.validate')
              }}</a-button>
              <a-popconfirm :title="t('managedFlink.publishConfirm')" @confirm="publish('publish')">
                <a-button
                  v-auth="'app:release'"
                  type="primary"
                  :disabled="lock || dirty || running || !can('PUBLISH_' + spec.type)"
                >
                  {{ t('managedFlink.publish') }}
                </a-button>
              </a-popconfirm>
              <span v-if="!can('PUBLISH_' + spec.type)">{{
                capabilities['PUBLISH_' + spec.type] || 'UNVERIFIED'
              }}</span>
            </a-space>
          </a-form>
        </a-tab-pane>
        <a-tab-pane key="run" :tab="t('managedFlink.runs')">
          <a-space class="mb-4" wrap>
            <a-select
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
            <a-checkbox v-if="restore === 'FROM_NEW' && run" v-model:checked="fresh">{{
              t('managedFlink.confirmNew')
            }}</a-checkbox>
            <a-select
              v-if="restore === 'FROM_SNAPSHOT'"
              v-model:value="snapshotId"
              style="min-width: 240px"
              :options="restoreChoices.map(({ id }) => ({ value: id }))"
            />
            <a-button
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
            </a-button>
            <a-popconfirm
              :title="t('managedFlink.stop')"
              @confirm="command('stop', { expectedRunId: run?.id, mode: 'IMMEDIATE' })"
            >
              <a-button v-auth="'app:cancel'" :disabled="lock || !running || !can('STOP')">{{
                t('managedFlink.stop')
              }}</a-button>
            </a-popconfirm>
            <a-popconfirm
              :title="t('managedFlink.snapshotStopConfirm')"
              @confirm="command('stop', { expectedRunId: run?.id, mode: 'AFTER_SNAPSHOT' })"
            >
              <a-button
                v-auth="'app:cancel'"
                :disabled="lock || !running || !can('SNAPSHOT_THEN_STOP')"
                >{{ t('managedFlink.snapshotStop') }}</a-button
              >
            </a-popconfirm>
            <a-button
              v-auth="'savepoint:trigger'"
              :disabled="lock || !running || !can('SNAPSHOT')"
              @click="command('snapshots', { expectedRunId: run?.id })"
              >{{ t('managedFlink.snapshot') }}</a-button
            >
          </a-space>
          <a-list :data-source="runs">
            <template #renderItem="{ item }"
              ><a-list-item
                >{{ item.provider_instance_id }} · {{ item.run_state }} ·
                {{ item.origin }}</a-list-item
              ></template
            >
          </a-list>
        </a-tab-pane>
        <a-tab-pane key="release" :tab="t('managedFlink.versions')">
          <a-space class="mb-4">
            <a-select
              v-model:value="sourceReleaseId"
              style="min-width: 180px"
              :options="
                releases
                  .filter((item) => item.status === 'PUBLISHED')
                  .map((item) => ({ value: item.id, label: 'R' + item.release_no }))
              "
            />
            <a-popconfirm :title="t('managedFlink.publishConfirm')" @confirm="publish('rollback')">
              <a-button
                v-auth="'app:release'"
                :disabled="lock || running || !sourceReleaseId || !can('PUBLISH_' + spec.type)"
              >
                {{ t('managedFlink.rollback') }}
              </a-button>
            </a-popconfirm>
          </a-space>
          <a-list :data-source="releases">
            <template #renderItem="{ item }"
              ><a-list-item
                >R{{ item.release_no }} · {{ item.status }} ·
                {{ item.provider_version }}</a-list-item
              ></template
            >
          </a-list>
        </a-tab-pane>
        <a-tab-pane key="snapshot" :tab="t('managedFlink.snapshots')">
          <a-list :data-source="snapshots">
            <template #renderItem="{ item }"
              ><a-list-item>{{ item.id }} · {{ item.state }}</a-list-item></template
            >
          </a-list>
        </a-tab-pane>
        <a-tab-pane key="operations" :tab="t('managedFlink.operations')">
          <a-list :data-source="operations">
            <template #renderItem="{ item }">
              <a-list-item>
                <a-button type="link" @click="details(item.id)"
                  >{{ item.command_type }} · {{ item.status }}</a-button
                >
              </a-list-item>
            </template>
          </a-list>
        </a-tab-pane>
        <a-tab-pane key="diagnostics" :tab="t('managedFlink.diagnostics')">
          <p>{{ t('managedFlink.logs') }} · {{ capabilities.LOG_SEARCH || 'UNVERIFIED' }}</p>
          <p>{{ t('managedFlink.metrics') }} · {{ capabilities.METRICS || 'UNVERIFIED' }}</p>
        </a-tab-pane>
      </a-tabs>
      <a-drawer
        :visible="Boolean(selected)"
        :title="selected?.id"
        width="480"
        @close="selected = undefined"
      >
        <p>{{ selected?.command_type }} · {{ selected?.status }}</p>
        <a-list :data-source="selected?.steps || []">
          <template #renderItem="{ item }">
            <a-list-item
              >{{ item.action }} · {{ item.state }} · {{ item.error_code }} ·
              {{ item.request_id }}</a-list-item
            >
          </template>
        </a-list>
        <a-button
          v-if="selected?.status === 'VERIFY_REQUIRED'"
          v-auth="'app:release'"
          :loading="busy"
          @click="selected && reconcile(selected.id)"
          >{{ t('managedFlink.reconcile') }}</a-button
        >
      </a-drawer>
    </template>
  </section>
</template>
