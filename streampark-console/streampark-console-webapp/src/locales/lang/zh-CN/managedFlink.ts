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

export default {
  configuration: 'Flink 配置（每行一条 key=value）',
  invalidConfiguration: '请使用 key=value 格式，每个配置键只能填写一次。',
  loadMetrics: '加载实例指标',
  title: '托管 Flink',
  create: '新建托管 Flink 作业',
  failed: '托管 Flink 请求失败',
  name: '名称',
  environment: '托管环境',
  type: '作业类型',
  save: '保存草稿',
  settings: '托管 Flink 设置',
  connection: '云账号连接',
  provider: '厂商',
  account: '云账号 ID',
  region: '地域',
  projectId: '项目 ID',
  directoryId: '目录 ID',
  resourcePoolId: '资源池 ID',
  resourcePoolName: '资源池名称',
  engineRef: '引擎标识',
  artifactBucket: 'TOS 桶',
  artifactPrefix: '制品前缀',
  tlsTopic: 'TLS Topic',
  tlsRegion: 'TLS 地域',
  saved: '草稿已保存',
  validated: '本地结构校验通过',
  draft: '草稿',
  mainClass: '主类',
  args: '程序参数，每行一项',
  validate: '校验',
  publish: '发布',
  publishConfirm: '确认发布此版本？实例需要单独启动。',
  runs: '实例',
  restartNew: '全新启动',
  latest: '从最新兼容快照恢复',
  snapshot: '快照',
  confirmNew: '确认全新启动，放弃恢复此前状态',
  start: '启动',
  stop: '停止实例',
  snapshotStop: '快照后停止',
  snapshotStopConfirm: '恢复后可能重放快照之后处理的数据。是否继续？',
  versions: '版本',
  rollback: '回滚发布',
  snapshots: '快照',
  operations: '操作记录',
  diagnostics: '诊断',
  logs: '日志',
  metrics: '指标',
  logInstance: '日志实例',
  logComponent: '组件',
  logLevel: '级别',
  logKeyword: '关键字或 RequestId',
  logFrom: '日志开始时间',
  logUntil: '日志结束时间',
  loadLogs: '查询实例日志',
  openVolcengineConsole: '打开火山引擎 Flink 控制台',
  nextLogPage: '下一页日志',
  logStatus: {
    AVAILABLE: '火山引擎 TLS 日志',
    NO_DATA: '该时段暂无已归档日志',
    PARTIAL: 'TLS 返回部分结果',
    TIMEOUT: 'TLS 查询超时',
    NO_ACCESS: '云身份无此日志主题的读取权限',
    EXPIRED: '所选时段已超出主题保留期限',
    THROTTLED: 'TLS 查询达到限流阈值',
    NEEDS_CONFIGURATION: '当前环境需要已验证的日志主题和索引',
    UNVERIFIED: '当前环境的 TLS 日志尚未验证',
    STALE: '日志暂时无法读取',
  },
  reconcile: '刷新对账证据',
  importBuild: '导入已构建项目的 JAR',
  module: '模块',
};
