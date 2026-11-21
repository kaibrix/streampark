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
    Modal,
    Radio,
    Select,
    Space,
  } from 'ant-design-vue';
  import { onMounted, reactive, ref } from 'vue';
  import { useI18n } from '/@/hooks/web/useI18n';
  import { useRouter } from 'vue-router';
  import { managedGet, managedPost, type Environment, type Spec } from '/@/api/flink/managedFlink';

  const CollapsePanel = Collapse.Panel;
  const FormItem = Form.Item;
  const InputPassword = Input.Password;
  const RadioGroup = Radio.Group;
  const Textarea = Input.TextArea;

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
      const credentials =
        connection.accessKey || connection.secretKey
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
        <FormItem v-if="job.type === 'SQL'" label="SQL">
          <Textarea v-model:value="job.sql" :rows="8" />
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
          <Button v-auth="'team:update'" @click="showSettings">{{
            t('managedFlink.settings')
          }}</Button>
          <Button @click="visible = false">{{ t('common.cancelText') }}</Button>
        </Space>
      </Form>
    </Card>
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
                :options="['volcengine', 'mock', 'alibaba-fixture'].map((value) => ({ value }))"
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
