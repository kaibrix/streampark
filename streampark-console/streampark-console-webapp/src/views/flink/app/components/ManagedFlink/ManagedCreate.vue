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
    Card,
    Collapse,
    Form,
    Input,
    List,
    Modal,
    Radio,
    Select,
    Space,
  } from 'ant-design-vue';
  import { onMounted, reactive, ref, watch } from 'vue';
  import { useI18n } from '/@/hooks/web/useI18n';
  import { useRouter } from 'vue-router';
  import { managedGet, managedPost, type Environment, type Spec } from '/@/api/flink/managedFlink';
  import { format } from '../../FlinkSqlFormatter';

  const CollapsePanel = Collapse.Panel;
  const FormItem = Form.Item;
  const InputPassword = Input.Password;
  const RadioGroup = Radio.Group;
  const Textarea = Input.TextArea;

  const { t } = useI18n();
  const router = useRouter();
  const visible = ref(false);
  const settings = ref(false);
  const providers = ref<string[]>([]);
  const showCloudJobs = ref(false);
  const cloudFilter = ref('');
  const inventory = ref<{
    status: string;
    jobs?: { Id: string; JobName: string; JobType: string; State: string }[];
    page: number;
    hasNext: boolean;
  }>();

  function listCloudJobs(page = 1) {
    showCloudJobs.value = true;
    void run(async () => {
      inventory.value = undefined;
      inventory.value = await managedGet('/environments/' + job.environmentId + '/cloud-jobs', {
        page,
        name: cloudFilter.value.trim() || undefined,
      });
    });
  }
  const available = ref(false);
  const busy = ref(false);
  const message = ref('');
  const environments = ref<Environment[]>([]);
  const connections = ref<{ id: string; name: string }[]>([]);
  const job = reactive({
    name: '',
    environmentId: '',
    type: 'SQL' as Spec['type'],
    batchMode: false,
    sql: '',
  });
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

  watch([settings, () => connection.provider], () => {
    connection.accessKey = '';
    connection.secretKey = '';
  });

  function showSettings() {
    void run(async () => {
      [connections.value, providers.value] = await Promise.all([
        managedGet<{ id: string; name: string }[]>('/connections'),
        managedGet<string[]>('/providers'),
      ]);
      settings.value = true;
    });
  }

  function addConnection() {
    void run(async () => {
      const credentials =
        connection.provider === 'volcengine' && (connection.accessKey || connection.secretKey)
          ? { accessKey: connection.accessKey, secretKey: connection.secretKey }
          : {};
      const id = await managedPost<string>('/connections', {
        name: connection.name,
        provider: connection.provider,
        accountRef: connection.accountRef,
        ...credentials,
      });
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
      job.environmentId = id;
      await probe(id);
    });
  }

  async function probe(id: string) {
    const report = await managedPost<Record<string, string>>(`/environments/${id}/probe`, {});
    message.value = Object.entries(report)
      .map(([key, value]) => `${key} ${value}`)
      .join(' · ');
  }

  function create() {
    void run(async () => {
      const spec: Spec = { type: job.type, args: [], configuration: {} };
      if (job.batchMode) spec.batchMode = true;
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
    <Button v-if="!visible" @click="visible = true">{{ t('managedFlink.create') }}</Button>
    <Card v-else :title="t('managedFlink.create')">
      <Alert v-if="message" class="mb-4" :message="message" show-icon />
      <Form :model="job" layout="vertical" @finish="create">
        <FormItem :label="t('managedFlink.name')" required>
          <Input v-model:value="job.name" />
        </FormItem>
        <FormItem :label="t('managedFlink.environment')" required>
          <Select
            v-model:value="job.environmentId"
            :options="
              environments
                .filter((item) => item.enabled)
                .map(({ id, name }) => ({ value: id, label: name }))
            "
          />
        </FormItem>
        <FormItem :label="t('managedFlink.type')">
          <RadioGroup v-model:value="job.type" :options="['SQL', 'JAR']" />
        </FormItem>
        <FormItem :label="t('managedFlink.processingMode')">
          <RadioGroup
            v-model:value="job.batchMode"
            :options="[
              { value: false, label: t('managedFlink.streaming') },
              { value: true, label: t('managedFlink.batch') },
            ]"
          />
        </FormItem>
        <FormItem v-if="job.type === 'SQL'" label="SQL">
          <Textarea v-model:value="job.sql" :rows="8" />
          <Button size="small" :disabled="!job.sql" @click="job.sql = format(job.sql)">
            {{ t('flink.app.flinkSql.format') }}
          </Button>
        </FormItem>
        <Space>
          <Button
            type="primary"
            html-type="submit"
            :loading="busy"
            :disabled="!job.name || !job.environmentId"
          >
            {{ t('managedFlink.save') }}
          </Button>
          <Button
            v-auth="'team:update'"
            :disabled="busy || !job.environmentId"
            @click="run(() => probe(job.environmentId))"
          >
            {{ t('managedFlink.probe') }}
          </Button>
          <Button
            v-if="
              environments.some(
                (env) => env.id === job.environmentId && env.provider === 'volcengine',
              )
            "
            v-auth="'team:update'"
            :disabled="busy"
            @click="listCloudJobs()"
            >{{ t('managedFlink.cloudJobs') }}</Button
          >
          <Button v-auth="'team:update'" @click="showSettings">{{
            t('managedFlink.settings')
          }}</Button>
          <Button @click="visible = false">{{ t('common.cancelText') }}</Button>
        </Space>
      </Form>
    </Card>
    <Modal v-model:visible="showCloudJobs" :title="t('managedFlink.cloudJobs')" :footer="null">
      <Input.Search
        v-model:value="cloudFilter"
        :placeholder="t('managedFlink.name')"
        :loading="busy"
        @search="listCloudJobs()"
      />
      <Alert
        v-if="inventory && inventory.status !== 'AVAILABLE'"
        :message="inventory.status"
        type="warning"
      />
      <template v-if="inventory?.status === 'AVAILABLE'">
        <List :data-source="inventory.jobs">
          <template #renderItem="{ item }"
            ><List.Item
              >{{ item.JobName }} · {{ item.Id }} · {{ item.JobType }} · {{ item.State }}</List.Item
            ></template
          >
        </List>
        <Space>
          <Button
            :disabled="busy || inventory.page <= 1"
            @click="listCloudJobs((inventory?.page || 1) - 1)"
            >{{ t('managedFlink.prevPage') }}</Button
          >
          <Button
            :disabled="busy || !inventory.hasNext"
            @click="listCloudJobs((inventory?.page || 1) + 1)"
            >{{ t('managedFlink.nextPage') }}</Button
          >
        </Space>
      </template>
    </Modal>
    <Modal
      v-model:visible="settings"
      :title="t('managedFlink.settings')"
      :footer="null"
      width="680px"
    >
      <Alert v-if="message" class="mb-4" :message="message" />
      <Collapse>
        <CollapsePanel key="connection" :header="t('managedFlink.connection')">
          <Form :model="connection" layout="vertical" @finish="addConnection">
            <FormItem :label="t('managedFlink.name')"
              ><Input v-model:value="connection.name"
            /></FormItem>
            <FormItem :label="t('managedFlink.provider')">
              <Select
                v-model:value="connection.provider"
                :options="providers.map((value) => ({ value }))"
              />
            </FormItem>
            <FormItem :label="t('managedFlink.account')"
              ><Input v-model:value="connection.accountRef"
            /></FormItem>
            <template v-if="connection.provider === 'volcengine'">
              <FormItem label="Access Key"
                ><InputPassword v-model:value="connection.accessKey" autocomplete="off"
              /></FormItem>
              <FormItem label="Secret Key"
                ><InputPassword v-model:value="connection.secretKey" autocomplete="off"
              /></FormItem>
            </template>
            <Button html-type="submit" :loading="busy">{{ t('managedFlink.connection') }}</Button>
          </Form>
        </CollapsePanel>
        <CollapsePanel key="environment" :header="t('managedFlink.environment')">
          <Form :model="environment" layout="vertical" @finish="addEnvironment">
            <FormItem :label="t('managedFlink.name')"
              ><Input v-model:value="environment.name"
            /></FormItem>
            <FormItem :label="t('managedFlink.connection')">
              <Select
                v-model:value="environment.connectionId"
                :options="connections.map(({ id, name }) => ({ value: id, label: name }))"
              />
            </FormItem>
            <FormItem :label="t('managedFlink.region')"
              ><Input v-model:value="environment.region"
            /></FormItem>
            <FormItem
              v-for="(_, key) in environment.target"
              :key="key"
              :label="t('managedFlink.' + key)"
            >
              <Input v-model:value="environment.target[key]" />
            </FormItem>
            <Button html-type="submit" :loading="busy">{{ t('managedFlink.environment') }}</Button>
          </Form>
        </CollapsePanel>
      </Collapse>
    </Modal>
  </div>
</template>
