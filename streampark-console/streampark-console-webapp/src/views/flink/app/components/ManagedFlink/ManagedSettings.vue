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
    Collapse,
    Form,
    Input,
    Modal,
    Radio,
    Select,
    Space,
    Switch,
  } from 'ant-design-vue';
  import { computed, onMounted, reactive, ref } from 'vue';
  import {
    managedGet,
    managedPatch,
    managedPost,
    readManagedCatalog,
    type Capabilities,
    type Connection,
    type Environment,
  } from '/@/api/flink/managedFlink';
  import { useI18n } from '/@/hooks/web/useI18n';

  const CollapsePanel = Collapse.Panel;
  const FormItem = Form.Item;
  const InputPassword = Input.Password;
  const RadioGroup = Radio.Group;
  const { t } = useI18n();
  const emit = defineEmits<{
    (event: 'close'): void;
    (event: 'environments', entries: Environment[], selectedId?: string): void;
  }>();

  const busy = ref(false);
  const error = ref('');
  const feedback = ref('');
  const providers = ref<string[]>([]);
  const connections = ref<Connection[]>([]);
  const environments = ref<Environment[]>([]);
  const connectionId = ref<string>();
  const environmentId = ref<string>();
  const rotationEnvironmentId = ref<string>();
  const report = ref<Capabilities>();

  const connection = reactive({
    name: '',
    provider: 'volcengine',
    accountRef: '',
    credentialMode: 'SERVER_PROFILE' as 'SERVER_PROFILE' | 'ACCESS_KEY',
    accessKey: '',
    secretKey: '',
  });
  const rotation = reactive({ accessKey: '', secretKey: '' });
  const defaultTarget = {
    projectId: '',
    directoryId: '',
    resourcePoolId: '',
    resourcePoolName: '',
    engineRef: '',
    artifactBucket: '',
    artifactPrefix: '',
    tlsTopic: '',
    tlsRegion: '',
  };
  const environment = reactive({
    name: '',
    connectionId: '',
    region: '',
    enabled: true,
    expectedConfigVersion: undefined as number | undefined,
    target: { ...defaultTarget },
  });
  const editingConnection = computed(() =>
    connections.value.find((item) => item.id === connectionId.value),
  );
  const editingEnvironment = computed(() =>
    environments.value.find((item) => item.id === environmentId.value),
  );
  const rotationEnvironments = computed(() =>
    environments.value.filter((item) => item.connectionId === connectionId.value),
  );
  const canRotate = computed(() => {
    const selected = editingConnection.value;
    return (
      selected?.provider === 'volcengine' &&
      (selected.credentialsManagedByServer === false || selected.credentialsManagedByServer === 0)
    );
  });
  const observedAt = computed(() => {
    const timestamp = Number(editingEnvironment.value?.capabilitiesObservedAt);
    return timestamp > 0 ? new Date(timestamp).toLocaleString() : '';
  });

  async function run(action: () => Promise<void>) {
    if (busy.value) return;
    busy.value = true;
    error.value = '';
    feedback.value = '';
    try {
      await action();
    } catch (cause) {
      error.value = (cause as Error).message || t('managedFlink.failed');
    } finally {
      busy.value = false;
    }
  }

  function clearConnectionSecrets() {
    connection.accessKey = '';
    connection.secretKey = '';
    rotation.accessKey = '';
    rotation.secretKey = '';
  }

  function resetCredentialMode() {
    connection.credentialMode = 'SERVER_PROFILE';
    clearConnectionSecrets();
  }

  function selectConnection(id?: string) {
    connectionId.value = id;
    rotationEnvironmentId.value = undefined;
    connection.name = editingConnection.value?.name || '';
    connection.provider = editingConnection.value?.provider || 'volcengine';
    connection.accountRef = editingConnection.value?.accountRef || '';
    resetCredentialMode();
  }

  function selectEnvironment(id?: string) {
    environmentId.value = id;
    report.value = undefined;
    const selected = editingEnvironment.value;
    environment.name = selected?.name || '';
    environment.connectionId = selected?.connectionId || connectionId.value || '';
    environment.region = selected?.region || '';
    environment.enabled = selected ? Boolean(selected.enabled) : true;
    environment.expectedConfigVersion = selected?.configVersion;
    const target = selected ? (JSON.parse(selected.target) as Partial<typeof defaultTarget>) : {};
    environment.target = { ...defaultTarget, ...target };
    if (id) {
      void run(async () => {
        report.value = await managedGet<Capabilities>(`/environments/${id}/capabilities`);
      });
    }
  }

  async function refreshEnvironments(selectedId?: string) {
    environments.value = await readManagedCatalog('/environments');
    emit('environments', environments.value, selectedId);
  }

  onMounted(() =>
    run(async () => {
      [connections.value, providers.value] = await Promise.all([
        readManagedCatalog<Connection>('/connections'),
        managedGet<string[]>('/providers'),
        refreshEnvironments(),
      ]);
    }),
  );

  function saveConnection() {
    void run(async () => {
      let id = connectionId.value;
      if (id) {
        await managedPatch(`/connections/${id}`, { name: connection.name });
      } else {
        const { name, provider, accountRef, credentialMode, accessKey, secretKey } = connection;
        const credentials =
          provider === 'volcengine' && credentialMode === 'ACCESS_KEY'
            ? { accessKey, secretKey }
            : {};
        id = await managedPost<string>('/connections', {
          name,
          provider,
          accountRef,
          ...credentials,
        });
        connectionId.value = id;
        if (!environmentId.value) environment.connectionId = id;
      }
      clearConnectionSecrets();
      feedback.value = t('managedFlink.connectionSaved');
      connections.value = await readManagedCatalog('/connections');
      selectConnection(id);
    });
  }

  function rotateConnection() {
    if (!connectionId.value || !rotationEnvironmentId.value || !canRotate.value) return;
    const path = `/connections/${connectionId.value}/rotate?environmentId=${encodeURIComponent(
      rotationEnvironmentId.value,
    )}`;
    void run(async () => {
      try {
        await managedPost<Capabilities>(path, { accountRef: connection.accountRef, ...rotation });
      } finally {
        clearConnectionSecrets();
      }
      feedback.value = t('managedFlink.credentialsRotated');
      connections.value = await readManagedCatalog('/connections');
      await refreshEnvironments();
      if (environmentId.value) {
        report.value = await managedGet<Capabilities>(
          `/environments/${environmentId.value}/capabilities`,
        );
      }
    });
  }

  function saveEnvironment() {
    void run(async () => {
      const id = environmentId.value
        ? await managedPatch<string>(`/environments/${environmentId.value}`, { ...environment })
        : await managedPost<string>('/environments', { ...environment });
      environmentId.value = id;
      feedback.value = t('managedFlink.environmentSaved');
      await refreshEnvironments(id);
      environment.expectedConfigVersion = editingEnvironment.value?.configVersion;
      await probeEnvironment(id);
    });
  }

  async function probeEnvironment(id: string) {
    report.value = await managedPost<Capabilities>(`/environments/${id}/probe`, {});
    await refreshEnvironments();
  }

  function probeSelectedEnvironment() {
    const id = environmentId.value;
    if (id) void run(() => probeEnvironment(id));
  }
</script>

<template>
  <Modal
    :visible="true"
    :title="t('managedFlink.settings')"
    :footer="null"
    :closable="!busy"
    :mask-closable="!busy"
    :keyboard="!busy"
    width="680px"
    @cancel="emit('close')"
  >
    <Alert v-if="error" class="mb-4" :message="error" type="error" show-icon />
    <Alert v-if="feedback" class="mb-4" :message="feedback" type="success" show-icon />
    <Collapse>
      <CollapsePanel key="connection" :header="t('managedFlink.connection')">
        <Form :model="connection" layout="vertical" @finish="saveConnection">
          <FormItem :label="t('managedFlink.existingConnection')">
            <Select
              :value="connectionId"
              :options="connections.map(({ id, name }) => ({ value: id, label: name }))"
              :placeholder="t('managedFlink.newConnection')"
              :disabled="busy"
              allow-clear
              @change="selectConnection"
            />
          </FormItem>
          <FormItem :label="t('managedFlink.name')" required>
            <Input v-model:value="connection.name" :disabled="busy" />
          </FormItem>
          <FormItem :label="t('managedFlink.provider')" required>
            <Select
              v-model:value="connection.provider"
              :options="providers.map((value) => ({ value }))"
              :disabled="busy || Boolean(connectionId)"
              @change="resetCredentialMode"
            />
          </FormItem>
          <FormItem :label="t('managedFlink.account')" required>
            <Input
              v-model:value="connection.accountRef"
              :disabled="busy || Boolean(connectionId)"
            />
          </FormItem>
          <template v-if="!connectionId && connection.provider === 'volcengine'">
            <FormItem :label="t('managedFlink.credentialMode')">
              <RadioGroup
                v-model:value="connection.credentialMode"
                :options="[
                  { value: 'SERVER_PROFILE', label: t('managedFlink.serverProfile') },
                  { value: 'ACCESS_KEY', label: 'Access Key / Secret Key' },
                ]"
                :disabled="busy"
                @change="clearConnectionSecrets"
              />
            </FormItem>
            <template v-if="connection.credentialMode === 'ACCESS_KEY'">
              <FormItem label="Access Key" required>
                <InputPassword
                  v-model:value="connection.accessKey"
                  :disabled="busy"
                  autocomplete="off"
                />
              </FormItem>
              <FormItem label="Secret Key" required>
                <InputPassword
                  v-model:value="connection.secretKey"
                  :disabled="busy"
                  autocomplete="off"
                />
              </FormItem>
            </template>
          </template>
          <Button
            html-type="submit"
            :loading="busy"
            :disabled="
              !connection.name.trim() ||
              !connection.accountRef.trim() ||
              !connection.provider ||
              (connection.credentialMode === 'ACCESS_KEY' &&
                (!connection.accessKey || !connection.secretKey))
            "
          >
            {{ t(connectionId ? 'managedFlink.saveConnection' : 'managedFlink.connection') }}
          </Button>
        </Form>
        <p v-if="editingConnection" class="mt-4">
          {{
            editingConnection.credentialsManagedByServer
              ? t('managedFlink.serverProfileCredentials')
              : t('managedFlink.credentialVersion', [editingConnection.credentialVersion])
          }}
        </p>
        <Form v-if="canRotate" :model="rotation" layout="vertical" @finish="rotateConnection">
          <FormItem :label="t('managedFlink.rotationEnvironment')" required>
            <Select
              v-model:value="rotationEnvironmentId"
              :disabled="busy"
              :options="rotationEnvironments.map(({ id, name }) => ({ value: id, label: name }))"
            />
          </FormItem>
          <FormItem label="Access Key" required>
            <InputPassword v-model:value="rotation.accessKey" :disabled="busy" autocomplete="off" />
          </FormItem>
          <FormItem label="Secret Key" required>
            <InputPassword v-model:value="rotation.secretKey" :disabled="busy" autocomplete="off" />
          </FormItem>
          <Button
            html-type="submit"
            :loading="busy"
            :disabled="!rotationEnvironmentId || !rotation.accessKey || !rotation.secretKey"
          >
            {{ t('managedFlink.rotateCredentials') }}
          </Button>
        </Form>
      </CollapsePanel>
      <CollapsePanel key="environment" :header="t('managedFlink.environment')">
        <Form :model="environment" layout="vertical" @finish="saveEnvironment">
          <FormItem :label="t('managedFlink.existingEnvironment')">
            <Select
              :value="environmentId"
              :options="environments.map(({ id, name }) => ({ value: id, label: name }))"
              :placeholder="t('managedFlink.newEnvironment')"
              :disabled="busy"
              allow-clear
              @change="selectEnvironment"
            />
          </FormItem>
          <FormItem :label="t('managedFlink.name')" required>
            <Input v-model:value="environment.name" :disabled="busy" />
          </FormItem>
          <FormItem :label="t('managedFlink.connection')" required>
            <Select
              v-model:value="environment.connectionId"
              :options="connections.map(({ id, name }) => ({ value: id, label: name }))"
              :disabled="busy || Boolean(environmentId)"
            />
          </FormItem>
          <FormItem :label="t('managedFlink.region')" required>
            <Input v-model:value="environment.region" :disabled="busy || Boolean(environmentId)" />
          </FormItem>
          <FormItem v-for="(_, key) in defaultTarget" :key="key" :label="t('managedFlink.' + key)">
            <Input
              v-model:value="environment.target[key]"
              :disabled="busy || (key === 'projectId' && Boolean(environmentId))"
            />
          </FormItem>
          <FormItem :label="t('managedFlink.environmentEnabled')">
            <Switch v-model:checked="environment.enabled" :disabled="busy" />
          </FormItem>
          <Space wrap>
            <Button html-type="submit" :loading="busy" :disabled="!environment.name.trim()">
              {{ t(environmentId ? 'managedFlink.saveEnvironment' : 'managedFlink.environment') }}
            </Button>
            <Button v-if="environmentId" :disabled="busy" @click="probeSelectedEnvironment">
              {{ t('managedFlink.probe') }}
            </Button>
          </Space>
        </Form>
        <p v-if="observedAt" class="mt-4">{{ t('managedFlink.lastProbe', [observedAt]) }}</p>
        <dl v-if="report" class="mt-4 grid grid-cols-2 gap-x-2">
          <template v-for="(status, capability) in report" :key="capability">
            <dt>{{ capability }}</dt>
            <dd>{{ status }}</dd>
          </template>
        </dl>
      </CollapsePanel>
    </Collapse>
  </Modal>
</template>
