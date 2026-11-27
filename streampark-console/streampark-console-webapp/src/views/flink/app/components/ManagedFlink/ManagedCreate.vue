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
    Form,
    Input,
    List,
    Modal,
    Radio,
    Select,
    Space,
  } from 'ant-design-vue';
  import { computed, onMounted, reactive, ref } from 'vue';
  import { useI18n } from '/@/hooks/web/useI18n';
  import { useRouter } from 'vue-router';
  import {
    catalogPageSize,
    managedGet,
    managedPost,
    readManagedCatalog,
    type Environment,
    type Spec,
  } from '/@/api/flink/managedFlink';
  import { format } from '../../FlinkSqlFormatter';
  import ManagedSettings from './ManagedSettings.vue';

  const FormItem = Form.Item;
  const RadioGroup = Radio.Group;
  const Textarea = Input.TextArea;
  const props = defineProps<{ copyFrom?: string }>();

  const { t } = useI18n();
  const router = useRouter();
  const visible = ref(Boolean(props.copyFrom));
  const copiedSpec = ref<Spec>();
  const settings = ref(false);
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
  const available = ref(Boolean(props.copyFrom));
  const busy = ref(false);
  const message = ref('');
  const environments = ref<Environment[]>([]);
  const job = reactive({
    name: '',
    environmentId: '',
    type: 'SQL' as Spec['type'],
    batchMode: false,
    sql: '',
  });
  const isVolcengine = computed(() =>
    environments.value.some((env) => env.id === job.environmentId && env.provider === 'volcengine'),
  );
  const invalidVolcengineName = computed(
    () =>
      isVolcengine.value &&
      !/^[A-Za-z0-9](?:[A-Za-z0-9_.-]{0,46}[A-Za-z0-9])?$/.test(job.name.trim()),
  );
  function showCreate() {
    visible.value = true;
    if (environments.value.length === catalogPageSize) {
      void run(async () => {
        environments.value = await readManagedCatalog('/environments', environments.value);
      });
    }
  }

  onMounted(async () => {
    if (props.copyFrom) {
      await run(async () => {
        const [entries, spec] = await Promise.all([
          readManagedCatalog<Environment>('/environments'),
          managedGet<Spec>(`/apps/${props.copyFrom}/copy-spec`),
        ]);
        environments.value = entries;
        copiedSpec.value = spec;
        job.type = spec.type;
        job.batchMode = Boolean(spec.batchMode);
        job.sql = spec.sql || '';
      });
      return;
    }
    try {
      environments.value = await managedGet<Environment[]>('/environments');
      available.value = true;
    } catch {
      available.value = false;
    }
  });

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

  function updateEnvironments(entries: Environment[], selectedId?: string) {
    environments.value = entries;
    const selection = selectedId || job.environmentId;
    job.environmentId = entries.some(({ id, enabled }) => id === selection && enabled)
      ? selection
      : '';
  }

  async function probe(id: string) {
    const report = await managedPost<Record<string, string>>(`/environments/${id}/probe`, {});
    message.value = Object.entries(report)
      .map(([key, value]) => `${key} ${value}`)
      .join(' · ');
  }

  function create() {
    if (invalidVolcengineName.value || (props.copyFrom && !copiedSpec.value)) return;
    void run(async () => {
      const spec: Spec = copiedSpec.value
        ? { ...copiedSpec.value }
        : { type: job.type, args: [], configuration: {} };
      if (job.batchMode) spec.batchMode = true;
      if (job.type === 'SQL') spec.sql = job.sql;
      const appId = await managedPost<string>('/apps', {
        name: job.name.trim(),
        environmentId: job.environmentId,
        spec,
      });
      await router.push({ path: '/flink/app/detail', query: { appId } });
    });
  }

  function cancelCreate() {
    if (props.copyFrom) {
      void router.push({ path: '/flink/app' });
    } else {
      visible.value = false;
    }
  }
</script>

<template>
  <div v-if="available" class="mb-4">
    <Button v-if="!visible" @click="showCreate">{{ t('managedFlink.create') }}</Button>
    <Card v-else :title="t(copyFrom ? 'managedFlink.copyAsManaged' : 'managedFlink.create')">
      <Alert v-if="message" class="mb-4" :message="message" show-icon />
      <Alert
        v-if="copyFrom && copiedSpec"
        class="mb-4"
        :message="t('managedFlink.copyArtifactNote')"
        show-icon
      />
      <Form :model="job" layout="vertical" @finish="create">
        <FormItem
          :label="t('managedFlink.name')"
          :validate-status="job.name && invalidVolcengineName ? 'error' : undefined"
          :help="
            job.name && invalidVolcengineName ? t('managedFlink.volcengineJobNameRule') : undefined
          "
          required
        >
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
          <RadioGroup v-model:value="job.type" :options="['SQL', 'JAR']" :disabled="!!copyFrom" />
        </FormItem>
        <FormItem :label="t('managedFlink.processingMode')">
          <RadioGroup
            v-model:value="job.batchMode"
            :disabled="!!copyFrom"
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
            :disabled="
              !job.name.trim() ||
              !job.environmentId ||
              invalidVolcengineName ||
              (!!copyFrom && !copiedSpec)
            "
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
            v-if="isVolcengine"
            v-auth="'team:update'"
            :disabled="busy"
            @click="listCloudJobs()"
            >{{ t('managedFlink.cloudJobs') }}</Button
          >
          <Button v-auth="'team:update'" :disabled="busy" @click="settings = true">{{
            t('managedFlink.settings')
          }}</Button>
          <Button @click="cancelCreate">{{ t('common.cancelText') }}</Button>
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
    <ManagedSettings v-if="settings" @close="settings = false" @environments="updateEnvironments" />
  </div>
</template>
