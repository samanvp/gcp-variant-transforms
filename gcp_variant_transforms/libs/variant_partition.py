# Copyright 2018 Google Inc.  All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License');
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

"""Encapsulates all partitioning logic used by VCF to BigQuery pipeline.

VariantPartition class basically returns an index for a given
(reference_name, pos) pair. The main utilization of this class is in
partition_for() function used by DataFlow pipeline.
This class has 2 main operating modes:
  1) Automatic: it will partition variants based on their reference_name
  2) Based on user provided config file: Users can parition output tables as
     they wish by providing a partition config file, example config files are
     available at gcp_variant_transforms/testing/data/misc/*.yaml
"""

from __future__ import absolute_import

from collections import defaultdict
import re
import sys
import intervaltree
from mmh3 import hash  # pylint: disable=no-name-in-module,redefined-builtin
import yaml

from apache_beam.io.filesystems import FileSystems

from gcp_variant_transforms.libs import genomic_region_parser

# A reg exp that will match to standard reference_names such as "chr01" or "13".
_CHROMOSOME_NAME_REGEXP = re.compile(r'^(chr)?([0-9][0-9]?)$')
# Partition 0 to 21 is reserved for common reference_names such as "chr1".
_RESERVED_AUTO_PARTITIONS = 22
# We try to assign each chromosome to a partition. The first 22 partitions
# [0, 22) are reserved for standard reference_names. Every other identifiers
# will be matched to next available partitions [22, 27).
_DEFAULT_NUM_PARTITIONS = _RESERVED_AUTO_PARTITIONS + 5

# At most 1000 partitions can be set as output of VariantTransform.
_MAX_NUM_PARTITIONS = 1000
# Each partition can contain at most 64 regions.
_MAX_NUM_REGIONS = 64
# A special literal for identifying residual partition's region name.
_RESIDUAL_REGION_LITERAL = 'residual'
_UNDEFINED_PARTITION_INDEX = -1

_TABLE_NAME_REGEXP = re.compile(r'^[a-zA-Z0-9_]*$')
# At most 100 output tables can be set as output of VariantTransform.
_MAX_NUM_OUTPUT_TABLES = 100
# Each output table can use up to 10 CHROM values to filter variants.
_MAX_NUM_CHROM_VALUES = 10

# yaml config file constants
_OUTPUT_TABLE = 'output_table'
_TABLE_NAME_SUFFIX = 'table_name_suffix'
_CHROM_VALUES = 'CHROM_values'
_TOTAL_BASE_PAIRS = 'total_base_pairs'


# BQ commands needed for migration to schema V2
BQ_GET_SCHEMA='bq show --schema --format=prettyjson {SOURCE_FULL_TABLE_ID} > {TEMP_SCHEMA_JSON}'
BQ_CREATE_PARTITIONED_TABLE='bq mk --table --range_partitioning=start_position,0,{TOTAL_BASE_PAIRS},{PARTITION_SIZE} --clustering_fields=end_position {DEST_FULL_TABLE_ID} {TEMP_SCHEMA_JSON}'
BQ_WRITE_TO_TABLE='bq query --nouse_legacy_sql --destination_table={DEST_FULL_TABLE_ID} --append_table --schema_update_option=ALLOW_FIELD_RELAXATION "SELECT * EXCEPT({DATE_COLUMN}) FROM `{SOURCE_FULL_TABLE_ID_DOT}`"'

class _ChromosomePartitioner(object):
  """Assigns partition indices to multiple regions inside a chromosome.

  This class logic is implemented using an interval tree, each region is
  considered as an interval and will be added to the interval tree. Note all
  regions must be pairwise disjoint, i.e. no overlapping interval is accepted.
  """

  def __init__(self):
    # Each instance contains multiple regions of one chromosome.
    self._interval_tree = intervaltree.IntervalTree()

  def add_region(self, start, end, partition_index):
    if start < 0:
      raise ValueError(
          'Start position on a region cannot be negative: {}'.format(start))
    if end <= start:
      raise ValueError('End position must be larger than start position: {} '
                       'vs {}'.format(end, start))
    if partition_index < 0:
      raise ValueError(
          'Index of a region cannot be negative {}'.format(partition_index))
    if self._interval_tree.overlaps_range(start, end):
      raise ValueError(
          'Cannot add overlapping region {}-{}'.format(start, end))
    # If everything goes well we add the new region to the interval tree.
    self._interval_tree.addi(start, end, partition_index)

  def get_partition_index(self, pos=0):
    """Finds a region that includes pos, if none _UNDEFINED_PARTITION_INDEX."""
    matched_regions = self._interval_tree.search(pos)
    # Ensure at most one region is matching to the give position.
    assert len(matched_regions) <= 1
    if len(matched_regions) == 1:
      return next(iter(matched_regions)).data
    else:
      return _UNDEFINED_PARTITION_INDEX

class VariantPartition(object):
  """Partition variants based on their reference_name and position.

  This class has 2 operating modes:
    1) No config file is given (config_file_path == None):
       Automatically partition variants based on their reference_name.
    2) A config file is given:
       partitions variants based on input config file.
  """

  def __init__(self, config_file_path=None):
    if _DEFAULT_NUM_PARTITIONS <= _RESERVED_AUTO_PARTITIONS:
      raise ValueError(
          '_DEFAULT_NUM_PARTITIONS must be > _RESERVED_AUTO_PARTITIONS')
    self._num_partitions = _DEFAULT_NUM_PARTITIONS
    # This variable determines the operation mode auto (default mode) vs config.
    self._config_file_path_given = False
    # Residual partition will contain all remaining variants that do not match
    # to any other partition.
    self._residual_partition_index = _UNDEFINED_PARTITION_INDEX
    self._should_keep_residual_partition = False
    self._ref_name_to_partitions_map = defaultdict(_ChromosomePartitioner)
    self._partition_names = {}

    if config_file_path:
      self._config_file_path_given = True
      self._parse_config(config_file_path)

  def _validate_config(self, config_file_path):
    # type: (str) -> None
    with FileSystems.open(config_file_path, 'r') as f:
      try:
        partition_configs = yaml.load(f)
      except yaml.YAMLError as e:
        raise ValueError('Invalid yaml file: %s' % str(e))
    if len(partition_configs) > _MAX_NUM_PARTITIONS:
      raise ValueError(
          'There can be at most {} partitions but given config file '
          'contains {}'.format(_MAX_NUM_PARTITIONS, len(partition_configs)))
    if not partition_configs:
      raise ValueError('There must be at least one partition in config file.')

    existing_partition_names = set()
    for partition_config in partition_configs:
      partition = partition_config.get('partition', None)
      if partition is None:
        raise ValueError('Wrong yaml file format, partition field missing.')
      regions = partition.get('regions', None)
      if regions is None:
        raise ValueError('Each partition must have at least one region.')
      if len(regions) > _MAX_NUM_REGIONS:
        raise ValueError('At most {} regions per partition, thie partition '
                         'contains {}'.format(_MAX_NUM_REGIONS, len(regions)))
      if not partition.get('partition_name', None):
        raise ValueError('Each partition must have partition_name field.')
      partition_name = partition.get('partition_name').strip()
      if not partition_name:
        raise ValueError('Partition name can not be empty string.')
      if partition_name in existing_partition_names:
        raise ValueError('Partition names must be unique, '
                         '{} is duplicated'.format(partition_name))
      existing_partition_names.add(partition_name)
    return partition_configs

  def _parse_config(self, config_file_path):
    # type: (str) -> None
    """Parses the given partition config file.

    Args:
      config_file_path: name of the input partition_config file.
    Raises:
      A ValueError if any of the expected config formats are violated.
    """
    def _is_residual_partition(regions):
      # type: (List[str]) -> bool
      return (len(regions) == 1 and
              regions[0].strip().lower() == _RESIDUAL_REGION_LITERAL)

    partition_configs = self._validate_config(config_file_path)

    self._num_partitions = len(partition_configs)
    for partition_index in range(self._num_partitions):
      partition = partition_configs[partition_index].get('partition')
      self._partition_names[partition_index] = (
          partition.get('partition_name').strip())
      regions = partition.get('regions', None)

      if _is_residual_partition(regions):
        if self._residual_partition_index != _UNDEFINED_PARTITION_INDEX:
          raise ValueError('There must be only one residual partition.')
        self._residual_partition_index = partition_index
        self._should_keep_residual_partition = True
        continue

      for r in regions:
        ref_name, start, end = genomic_region_parser.parse_genomic_region(r)
        ref_name = ref_name.lower()
        self._ref_name_to_partitions_map[ref_name].add_region(
            start, end, partition_index)

    if self._residual_partition_index == _UNDEFINED_PARTITION_INDEX:
      # We add an extra dummy partition for residuals.
      # Note, here self._should_keep_residual_partition is False.
      self._residual_partition_index = self._num_partitions
      self._num_partitions += 1

  def get_num_partitions(self):
    # type: (None) -> int
    return self._num_partitions

  def get_partition(self, reference_name, pos=0):
    # type: (str, Optional[int]) -> int
    """Returns partition index on ref_name chromosome which pos falls into ."""
    reference_name = reference_name.strip().lower()
    if not reference_name or pos < 0:
      raise ValueError(
          'Cannot partition given input {}:{}'.format(reference_name, pos))
    if self._config_file_path_given:
      return self._get_config_partition(reference_name, pos)
    else:
      return self._get_auto_partition(reference_name)

  def _get_config_partition(self, reference_name, pos):
    # type: (str, int) -> int
    partitioner = self._ref_name_to_partitions_map.get(reference_name, None)
    if partitioner:
      partition_index = partitioner.get_partition_index(pos)
      if partition_index != _UNDEFINED_PARTITION_INDEX:
        return partition_index
    # No match was found, returns residual partition index.
    return self._residual_partition_index

  def _get_auto_partition(self, reference_name):
    # type: (str) -> int
    """Automatically chooses an partition for the given reference_name.

    Given a reference_name returns an index in [0, _DEFAULT_NUM_PARTITIONS)
    range. In order to make this lookup less computationally intensive we first:
      1) Lookup the reference_name in _ref_name_to_partitions_map dict

    If the result of lookup is None, we will try the following steps:
      2) Match the reference_name to a reg exp of common names (e.g. 'chr12') or
      3) Hash the reference_name and calculate its mod to remaining buckets
    result of 2-3 is added to _ref_name_to_partitions_map for future lookups.

    Args:
      reference_name: reference name of the variant which is being partitioned
    Returns:
      An integer in the range of [0, _DEFAULT_NUM_PARTITIONS)
    """
    partitioner = self._ref_name_to_partitions_map.get(reference_name, None)
    if partitioner:
      return partitioner.get_partition_index()
    else:
      matched = _CHROMOSOME_NAME_REGEXP.match(reference_name)
      if matched:
        # First match the reference_name to the common formats.
        _, chr_no = matched.groups()
        chr_no = int(chr_no)
        if chr_no > 0 and chr_no <= _RESERVED_AUTO_PARTITIONS:
          partition_index = chr_no - 1
          self._ref_name_to_partitions_map[reference_name].add_region(
              0, sys.maxint, partition_index)
          return partition_index
      # If RegExp didn't match, we will find the hash of reference_name
      remaining_partitions = _DEFAULT_NUM_PARTITIONS - _RESERVED_AUTO_PARTITIONS
      partition_index = (hash(reference_name) % remaining_partitions +
                         _RESERVED_AUTO_PARTITIONS)
      # Save partition in _reference_name_to_partition dict for future lookups
      self._ref_name_to_partitions_map[reference_name].add_region(
          0, sys.maxint, partition_index)
      return partition_index

  def should_flatten(self):
    # type: (None) -> bool
    """In auto mode (no config) flattens partitions, produces 1 output table."""
    return not self._config_file_path_given

  def should_keep_partition(self, partition_index):
    # type: (int) -> bool
    """Returns False only for dummy extra residual partition (if was added)."""
    if partition_index != self._residual_partition_index:
      return True
    else:
      return self._should_keep_residual_partition

  def get_partition_name(self, partition_index):
    # type: (int) -> Optional[str]
    if self._config_file_path_given:
      if partition_index >= self._num_partitions or partition_index < 0:
        raise ValueError(
            'Given partition index {} is outside of expected range: '
            '[0, {}]'.format(partition_index, self._num_partitions))
      return self._partition_names[partition_index]
    else:
      return None


class _ConfigParser(object):
  """Parsers partitioning yaml config file."""

  def __init__(self, config_file_path=None):
    if not config_file_path or not config_file_path.strip():
      raise ValueError('You must provide path to a yaml config file.')

    # Residual partition will contain all remaining variants that do not match
    # to any other partition.
    self._num_output_tables = 0
    self._residual_index = _UNDEFINED_PARTITION_INDEX
    self._should_keep_residual = False
    self._chrom_to_output_table_index = {}
    self._table_name_suffixes = []
    self._total_base_pairs = []

    self._parse_config(config_file_path)

  def _is_residual_table(self, chrom_values):
    # type: (List[str]) -> bool
    return (len(chrom_values) == 1 and
            chrom_values[0].strip().lower() == _RESIDUAL_REGION_LITERAL)

  def _validate_config(self, config_file_path):
    # type: (str) -> None
    with FileSystems.open(config_file_path, 'r') as f:
      try:
        output_tables = yaml.load(f)
      except yaml.YAMLError as e:
        raise ValueError('Invalid yaml file: {} .'.format(str(e)))
    if len(output_tables) > _MAX_NUM_OUTPUT_TABLES:
      raise ValueError(
          'There can be at most {} output tables but given config file '
          'contains {} .'.format(_MAX_NUM_OUTPUT_TABLES, len(output_tables)))
    if not output_tables:
      raise ValueError('At least one output table is needed in config file.')

    existing_suffixes = set()
    existing_chrom_values = set()
    residual_partition_index = _UNDEFINED_PARTITION_INDEX
    for item in output_tables:
      output_table = item.get(_OUTPUT_TABLE, None)
      if output_table is None:
        raise ValueError('Wrong yaml file format, {} field missing.'.format(
          _OUTPUT_TABLE))
      # Validate table_name_suffix
      table_name_suffix = output_table.get(_TABLE_NAME_SUFFIX)
      if not table_name_suffix:
        raise ValueError('Wrong yaml file format, {} field missing.'.format(
          _TABLE_NAME_SUFFIX))
      table_name_suffix = table_name_suffix.strip()
      if not table_name_suffix:
        raise ValueError('table_name_suffix can not be empty string.')
      if not _TABLE_NAME_REGEXP.match(table_name_suffix):
        raise ValueError('BigQuery table name can only contain letters (upper '
                         'or lower case), numbers, and underscores.')
      if table_name_suffix in existing_suffixes:
        raise ValueError('Table name suffixes must be unique, '
                         '{} is duplicated.'.format(table_name_suffix))
      existing_suffixes.add(table_name_suffix)
      # Validate CHROM_values
      chrom_values = output_table.get(_CHROM_VALUES, None)
      if chrom_values is None:
        raise ValueError('Wrong yaml file format, {} field missing.'.format(
          _CHROM_VALUES))
      if len(chrom_values) > _MAX_NUM_CHROM_VALUES:
        raise ValueError(
          'At most {} CHROM values per output table is allowed: {}.'.format(_MAX_NUM_CHROM_VALUESS, chrom_values))
      if self._is_residual_table(chrom_values):
        if residual_partition_index != _UNDEFINED_PARTITION_INDEX:
          raise ValueError('There can be only one residual output table.')
        residual_partition_index += 1
      for value in chrom_values:
        value = value.strip().lower()
        if not value:
          raise ValueError('CHROM_value can not be empty string.')
        if value in existing_chrom_values:
          raise ValueError(
            'chrom_values must be unique in config file: {} .'.format(value))
        existing_chrom_values.add(value)

      # Validate total_base_pairs
      total_base_pairs = output_table.get(_TOTAL_BASE_PAIRS, None)
      if not total_base_pairs:
        raise ValueError('Wrong yaml file format, {} field missing.'.format(
          _TOTAL_BASE_PAIRS))
      if type(total_base_pairs) is not int or total_base_pairs <= 0:
        raise ValueError('Each output table needs an int total_base_pairs > 0.')
    return output_tables

  def _parse_config(self, config_file_path):
    # type: (str) -> None
    """Parses the given partitioning config file.

    Args:
      config_file_path: name of the input partition_config file.
    Raises:
      A ValueError if any of the expected config formats are violated.
    """
    output_tables = self._validate_config(config_file_path)

    self._num_output_tables = len(output_tables)
    for table_index in range(self._num_output_tables):
      output_table = output_tables[table_index].get(_OUTPUT_TABLE)
      # Store table_name_suffix
      self._table_name_suffixes.insert(
        table_index, output_table.get(_TABLE_NAME_SUFFIX).strip())
      # Store chrom_values
      chrom_values = output_table.get(_CHROM_VALUES, None)
      if self._is_residual_table(chrom_values):
        self._residual_index = table_index
        self._should_keep_residual = True
        continue
      for values in chrom_values:
        values = values.strip().lower()
        self._chrom_to_output_table_index[values] = table_index
      # Store num_base_pairs
      self._total_base_pairs.insert(table_index,
                                    output_table.get(_TOTAL_BASE_PAIRS))

    if self._residual_index == _UNDEFINED_PARTITION_INDEX:
      # We add an extra dummy partition for residuals.
      # Note, here self._should_keep_residual is False.
      self._residual_index = self._num_output_tables
      self._num_output_tables += 1

  def get_num_partitions(self):
    # type: (None) -> int
    return self._num_output_tables

  def get_output_table_index(self, chrom):
    # type: (str) -> int
    """Returns output table index for the given chrom value."""
    index = self._chrom_to_output_table_index.get(chrom, None)
    if not index:
      index = self._residual_index
    return index

  def should_keep_output_table(self, output_table_index):
    # type: (int) -> bool
    """Returns False only for dummy extra residual partition (if was added)."""
    if output_table_index != self._residual_index:
      return True
    else:
      return self._should_keep_residual

  def _is_index_in_the_range(self, output_table_index):
    if output_table_index < 0:
      return False
    if self._should_keep_residual:
      if output_table_index >= self._num_output_tables:
        return False
    else:
      if output_table_index >= self._num_output_tables - 1:
        return False
    return True

  def get_output_table_suffix(self, output_table_index):
    # type: (int) -> Optional[str]
    if not self._is_index_in_the_range(partition_index):
      raise ValueError(
        'Given output index {} is outside of expected range: '
        '[0, {}]'.format(output_table_index, self._num_output_tables))
    return self._table_name_suffixes[output_table_index]

  def get_output_table_num_base_pairs(self, output_table_index):
    # type: (int) -> Optional[int]
    if not self._is_index_in_the_range(partition_index):
      raise ValueError(
        'Given output index {} is outside of expected range: '
        '[0, {}]'.format(output_table_index, self._num_output_tables))
    return self._num_base_paris[output_table_index]

