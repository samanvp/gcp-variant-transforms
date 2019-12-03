# Copyright 2019 Google LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Generates sample_info table schema."""

import math

from apache_beam.io.gcp.internal.clients import bigquery

from gcp_variant_transforms.libs import bigquery_util

SAMPLE_ID = 'sample_id'
SAMPLE_NAME = 'sample_name'
FILE_PATH = 'file_path'
TABLE_SUFFIX = 'sample_info'


def compose_table_name(base_name, suffix):
  # type: (str, List[str]) -> str
  return '_'.join([base_name, suffix])

_TOTAL_BASE_PAIRS_SIG_DIGITS = 4
_PARTITION_SIZE_SIG_DIGITS = 1
_NUM_BQ_RANGE_PARTITIONS = 4000

def calculate_optimize_partition_size(total_base_pairs):
  # These two operations adds [10^4, 2 * 10^4) buffer to total_base_pairs.
  total_base_pairs += math.pow(10, _TOTAL_BASE_PAIRS_SIG_DIGITS)
  total_base_pairs = (
      math.ceil(total_base_pairs / math.pow(10, _TOTAL_BASE_PAIRS_SIG_DIGITS)) *
      math.pow(10, _TOTAL_BASE_PAIRS_SIG_DIGITS))
  # We use 4000 - 1 = 3999 partitions just to avoid hitting the BQ limits.
  partition_size = total_base_pairs / (_NUM_BQ_RANGE_PARTITIONS - 1)
  # This operation adds another [0, 10 * 3999) buffer to the total_base_pairs.
  return (math.ceil(partition_size / pow(10, _PARTITION_SIZE_SIG_DIGITS)) *
          math.pow(10, _PARTITION_SIZE_SIG_DIGITS))

def generate_schema():
  # type: () -> bigquery.TableSchema
  schema = bigquery.TableSchema()
  schema.fields.append(bigquery.TableFieldSchema(
      name=SAMPLE_ID,
      type=bigquery_util.TableFieldConstants.TYPE_INTEGER,
      mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
      description='An Integer that uniquely identifies a sample.'))
  schema.fields.append(bigquery.TableFieldSchema(
      name=SAMPLE_NAME,
      type=bigquery_util.TableFieldConstants.TYPE_STRING,
      mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
      description=('Name of the sample as we read it from the VCF file.')))
  schema.fields.append(bigquery.TableFieldSchema(
      name=FILE_PATH,
      type=bigquery_util.TableFieldConstants.TYPE_STRING,
      mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
      description=('Full file path on GCS of the sample.')))

  return schema
