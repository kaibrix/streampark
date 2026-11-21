-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--    http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

CREATE TABLE smoke_input (
  sequence_id BIGINT
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '1',
  'fields.sequence_id.kind' = 'sequence',
  'fields.sequence_id.start' = '1',
  'fields.sequence_id.end' = '10000'
);

CREATE TABLE smoke_output (
  build_id STRING,
  sequence_id BIGINT
) WITH (
  'connector' = 'print'
);

INSERT INTO smoke_output
SELECT 'sql-v1', sequence_id FROM smoke_input;
