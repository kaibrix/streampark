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
  import { onMounted, reactive, ref } from 'vue';
  import { useI18n } from '/@/hooks/web/useI18n';
  import { useRouter } from 'vue-router';
  import { managedGet, managedPost, type Environment, type Spec } from '/@/api/flink/managedFlink';

  const { t } = useI18n();
  const router = useRouter();
  const visible = ref(false);
  const settings = ref(false);
  const available = ref(false);
  const busy = ref(false);
  const message = ref('');
  const environments = ref<Environment[]>([]);
  const connections = ref<{ id: string; name: string }[]>([]);
  const job = reactive({ name: '', environmentId: '', type: 'SQL' as Spec['type'], sql: '' });
  const connection = reactive({
    name: '',
    provider: 'volcengine',
    accountRef: '',
    accessKey: '',
    secretKey: '',
  });
  const environment = reactive({
    name: '',
    connectionId: '',
    region: '',
    target: {
      projectId: '',
      directoryId: '',
      resourcePoolId: '',
      resourcePoolName: '',
      engineRef: '',
      artifactBucket: '',
      artifactPrefix: '',
      tlsTopic: '',
      tlsRegion: '',
    },
  });

  async function refresh() {
    environments.value = await managedGet<Environment[]>('/environments');
    available.value = true;
  }

  onMounted(() =>
    refresh().catch(() => {
      available.value = false;
    }),
  );

  async function run(action: () => Promise<void>) {
    busy.value = true;
    message.value = '';
    try {
      await action();
    } catch (error) {
      message.value = (error as Error).message || t('managedFlink.failed');
    } finally {
      busy.value = false;
    }
  }

  function showSettings() {
    void run(async () => {
      connections.value = await managedGet('/connections');
      settings.value = true;
    });
  }

  function addConnection() {
    void run(async () => {
      const id = await managedPost<string>('/connections', { ...connection });
      connection.accessKey = '';
      connection.secretKey = '';
      connections.value = await managedGet('/connections');
      environment.connectionId = id;
    });
  }

  function addEnvironment() {
    void run(async () => {
      const id = await managedPost<string>('/environments', { ...environment });
      await refresh();
      const report = await managedPost<Record<string, string>>(`/environments/${id}/probe`, {});
      message.value = Object.entries(report)
        .map(([key, value]) => `${key} ${value}`)
        .join(' · ');
      job.environmentId = id;
    });
  }

  function create() {
    void run(async () => {
      const spec: Spec = { type: job.type, args: [], configuration: {} };
      if (job.type === 'SQL') spec.sql = job.sql;
      const appId = await managedPost<string>('/apps', {
        name: job.name,
        environmentId: job.environmentId,
        spec,
      });
      await router.push({ path: '/flink/app/detail', query: { appId } });
    });
  }
</script>

<template>
  <div v-if="available" class="mb-4">
    <a-button v-if="!visible" @click="visible = true">{{ t('managedFlink.create') }}</a-button>
    <a-card v-else :title="t('managedFlink.create')">
      <a-alert v-if="message" class="mb-4" :message="message" show-icon />
      <a-form layout="vertical" @finish="create">
        <a-form-item :label="t('managedFlink.name')" required>
          <a-input v-model:value="job.name" />
        </a-form-item>
        <a-form-item :label="t('managedFlink.environment')" required>
          <a-select
            v-model:value="job.environmentId"
            :options="
              environments
                .filter((item) => item.enabled)
                .map(({ id, name }) => ({ value: id, label: name }))
            "
          />
        </a-form-item>
        <a-form-item :label="t('managedFlink.type')">
          <a-radio-group v-model:value="job.type" :options="['SQL', 'JAR']" />
        </a-form-item>
        <a-form-item v-if="job.type === 'SQL'" label="SQL">
          <a-textarea v-model:value="job.sql" :rows="8" />
        </a-form-item>
        <a-space>
          <a-button
            type="primary"
            html-type="submit"
            :loading="busy"
            :disabled="!job.name || !job.environmentId"
          >
            {{ t('managedFlink.save') }}
          </a-button>
          <a-button v-auth="'team:update'" @click="showSettings">{{
            t('managedFlink.settings')
          }}</a-button>
          <a-button @click="visible = false">{{ t('common.cancelText') }}</a-button>
        </a-space>
      </a-form>
    </a-card>
    <a-modal
      v-model:visible="settings"
      :title="t('managedFlink.settings')"
      :footer="null"
      width="680px"
    >
      <a-alert v-if="message" class="mb-4" :message="message" />
      <a-collapse>
        <a-collapse-panel key="connection" :header="t('managedFlink.connection')">
          <a-form layout="vertical" @finish="addConnection">
            <a-form-item :label="t('managedFlink.name')"
              ><a-input v-model:value="connection.name"
            /></a-form-item>
            <a-form-item :label="t('managedFlink.provider')">
              <a-select
                v-model:value="connection.provider"
                :options="['volcengine', 'mock', 'alibaba-fixture'].map((value) => ({ value }))"
              />
            </a-form-item>
            <a-form-item :label="t('managedFlink.account')"
              ><a-input v-model:value="connection.accountRef"
            /></a-form-item>
            <template v-if="connection.provider === 'volcengine'">
              <a-form-item label="Access Key"
                ><a-input-password v-model:value="connection.accessKey" autocomplete="off"
              /></a-form-item>
              <a-form-item label="Secret Key"
                ><a-input-password v-model:value="connection.secretKey" autocomplete="off"
              /></a-form-item>
            </template>
            <a-button html-type="submit" :loading="busy">{{
              t('managedFlink.connection')
            }}</a-button>
          </a-form>
        </a-collapse-panel>
        <a-collapse-panel key="environment" :header="t('managedFlink.environment')">
          <a-form layout="vertical" @finish="addEnvironment">
            <a-form-item :label="t('managedFlink.name')"
              ><a-input v-model:value="environment.name"
            /></a-form-item>
            <a-form-item :label="t('managedFlink.connection')">
              <a-select
                v-model:value="environment.connectionId"
                :options="connections.map(({ id, name }) => ({ value: id, label: name }))"
              />
            </a-form-item>
            <a-form-item :label="t('managedFlink.region')"
              ><a-input v-model:value="environment.region"
            /></a-form-item>
            <a-form-item
              v-for="(_, key) in environment.target"
              :key="key"
              :label="t('managedFlink.' + key)"
            >
              <a-input v-model:value="environment.target[key]" />
            </a-form-item>
            <a-button html-type="submit" :loading="busy">{{
              t('managedFlink.environment')
            }}</a-button>
          </a-form>
        </a-collapse-panel>
      </a-collapse>
    </a-modal>
  </div>
</template>
