# Copyright 2018 Google Inc.  All Rights Reserved.
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

"""Unit tests for variant_partition module."""

from __future__ import absolute_import

import unittest

from gcp_variant_transforms.libs import variant_partition
from gcp_variant_transforms.testing import temp_dir

class VariantPartitionTest(unittest.TestCase):

  def test_auto_partitioning(self):
    partitioner = variant_partition.VariantPartition()
    self.assertTrue(partitioner.should_flatten())
    self.assertEqual(partitioner.get_num_partitions(),
                     variant_partition._DEFAULT_NUM_PARTITIONS)

    # Checking standard reference_name formatted as: 'chr[0-9][0-9]'
    for i in xrange(variant_partition._RESERVED_AUTO_PARTITIONS):
      self.assertEqual(partitioner.get_partition('chr' + str(i + 1)), i)
    # Checking standard reference_name formatted as: '[0-9][0-9]'
    for i in xrange(variant_partition._RESERVED_AUTO_PARTITIONS):
      self.assertEqual(partitioner.get_partition(str(i + 1)), i)

    # Every other reference_name will be assigned to partitions >= 22
    self.assertGreaterEqual(partitioner.get_partition('chrY'),
                            variant_partition._RESERVED_AUTO_PARTITIONS)
    self.assertGreaterEqual(partitioner.get_partition('chrX'),
                            variant_partition._RESERVED_AUTO_PARTITIONS)
    self.assertGreaterEqual(partitioner.get_partition('chrM'),
                            variant_partition._RESERVED_AUTO_PARTITIONS)
    self.assertGreaterEqual(partitioner.get_partition('chr23'),
                            variant_partition._RESERVED_AUTO_PARTITIONS)
    self.assertGreaterEqual(partitioner.get_partition('chr30'),
                            variant_partition._RESERVED_AUTO_PARTITIONS)
    self.assertGreaterEqual(partitioner.get_partition('Unknown'),
                            variant_partition._RESERVED_AUTO_PARTITIONS)
    # Expected empty string as partition_name as we are in auto mode.
    self.assertEqual(partitioner.get_partition_name(0), None)
    self.assertEqual(partitioner.get_partition_name(100), None)


  def test_auto_partitioning_invalid_partitions(self):
    partitioner = variant_partition.VariantPartition()
    self.assertTrue(partitioner.should_flatten())
    self.assertEqual(partitioner.get_num_partitions(),
                     variant_partition._DEFAULT_NUM_PARTITIONS)

    with self.assertRaisesRegexp(ValueError, 'Cannot partition given input*'):
      partitioner.get_partition('chr1', -1)

    with self.assertRaisesRegexp(ValueError, 'Cannot partition given input*'):
      partitioner.get_partition('', 1)

    with self.assertRaisesRegexp(ValueError, 'Cannot partition given input*'):
      partitioner.get_partition('  ', 1)

  def test_config_boundaries(self):
    partitioner = variant_partition.VariantPartition(
        'gcp_variant_transforms/testing/data/partition_configs/'
        'residual_at_end.yaml')
    self.assertFalse(partitioner.should_flatten())
    self.assertEqual(partitioner.get_num_partitions(), 8)
    for i in range(partitioner.get_num_partitions()):
      self.assertTrue(partitioner.should_keep_partition(i))

    # 'chr1:0-1,000,000'
    self.assertEqual(partitioner.get_partition('chr1', 0), 0)
    self.assertEqual(partitioner.get_partition('chr1', 999999), 0)
    # 'chr1:1,000,000-2,000,000'
    self.assertEqual(partitioner.get_partition('chr1', 1000000), 1)
    self.assertEqual(partitioner.get_partition('chr1', 1999999), 1)
    # 'chr1:2,000,000-999,999,999'
    self.assertEqual(partitioner.get_partition('chr1', 2000000), 2)
    self.assertEqual(partitioner.get_partition('chr1', 999999998), 2)
    self.assertEqual(partitioner.get_partition('chr1', 999999999), 7)

    # 'chr2' OR 'chr2_alternate_name1' OR 'chr2_alternate_name2' OR '2'.
    self.assertEqual(partitioner.get_partition('chr2', 0), 3)
    self.assertEqual(partitioner.get_partition('chr2', 999999999000), 3)
    self.assertEqual(
        partitioner.get_partition('chr2_alternate_name1', 0), 3)
    self.assertEqual(
        partitioner.get_partition('chr2_alternate_name1', 999999999000), 3)
    self.assertEqual(partitioner.get_partition('chr2_alternate_name2', 0), 3)
    self.assertEqual(
        partitioner.get_partition('CHR2_ALTERNATE_NAME2', 999999999000), 3)
    self.assertEqual(partitioner.get_partition('2', 0), 3)
    self.assertEqual(partitioner.get_partition('2', 999999999000), 3)

    # 'chr4' OR 'chr5' OR 'chr6:1,000,000-2,000,000'
    self.assertEqual(partitioner.get_partition('chr4', 0), 4)
    self.assertEqual(partitioner.get_partition('chr4', 999999999000), 4)
    self.assertEqual(partitioner.get_partition('chr5', 0), 4)
    self.assertEqual(partitioner.get_partition('chr5', 999999999000), 4)
    self.assertEqual(partitioner.get_partition('chr6', 1000000), 4)
    self.assertEqual(partitioner.get_partition('chr6', 2000000 - 1), 4)
    self.assertEqual(partitioner.get_partition('chr6', 0), 7)
    self.assertEqual(partitioner.get_partition('chr6', 999999), 7)
    self.assertEqual(partitioner.get_partition('chr6', 2000000), 7)

    # '3:0-500,000'
    self.assertEqual(partitioner.get_partition('3', 0), 5)
    self.assertEqual(partitioner.get_partition('3', 499999), 5)
    # '3:500,000-1,000,000'
    self.assertEqual(partitioner.get_partition('3', 500000), 6)
    self.assertEqual(partitioner.get_partition('3', 999999), 6)
    self.assertEqual(partitioner.get_partition('3', 1000000), 7)

  def test_config_case_insensitive(self):
    partitioner = variant_partition.VariantPartition(
        'gcp_variant_transforms/testing/data/partition_configs/'
        'residual_at_end.yaml')
    self.assertFalse(partitioner.should_flatten())
    self.assertEqual(partitioner.get_num_partitions(), 8)
    for i in range(partitioner.get_num_partitions()):
      self.assertTrue(partitioner.should_keep_partition(i))

    # 'chr1:0-1,000,000'
    self.assertEqual(partitioner.get_partition('chr1', 0), 0)
    self.assertEqual(partitioner.get_partition('Chr1', 0), 0)
    self.assertEqual(partitioner.get_partition('CHr1', 0), 0)
    self.assertEqual(partitioner.get_partition('CHR1', 0), 0)

  def test_config_get_partition_name(self):
    partitioner = variant_partition.VariantPartition(
        'gcp_variant_transforms/testing/data/partition_configs/'
        'residual_at_end.yaml')
    self.assertFalse(partitioner.should_flatten())
    self.assertEqual(partitioner.get_num_partitions(), 8)
    for i in range(partitioner.get_num_partitions()):
      self.assertTrue(partitioner.should_keep_partition(i))

    self.assertEqual(partitioner.get_partition_name(0), 'chr01_part1')
    self.assertEqual(partitioner.get_partition_name(1), 'chr01_part2')
    self.assertEqual(partitioner.get_partition_name(2), 'chr01_part3')
    self.assertEqual(partitioner.get_partition_name(3), 'chrom02')
    self.assertEqual(partitioner.get_partition_name(4), 'chrom04_05_part_06')
    self.assertEqual(partitioner.get_partition_name(5), 'chr3_01')
    self.assertEqual(partitioner.get_partition_name(6), 'chr3_02')
    self.assertEqual(partitioner.get_partition_name(7), 'all_remaining')


  def test_config_non_existent_partition_name(self):
    partitioner = variant_partition.VariantPartition(
        'gcp_variant_transforms/testing/data/partition_configs/'
        'residual_at_end.yaml')
    self.assertFalse(partitioner.should_flatten())
    self.assertEqual(partitioner.get_num_partitions(), 8)

    with self.assertRaisesRegexp(
        ValueError, 'Given partition index -1 is outside of expected range*'):
      partitioner.get_partition_name(-1)
    with self.assertRaisesRegexp(
        ValueError, 'Given partition index 8 is outside of expected range*'):
      partitioner.get_partition_name(8)

  def test_config_residual_partition_in_middle(self):
    partitioner = variant_partition.VariantPartition(
        'gcp_variant_transforms/testing/data/partition_configs/'
        'residual_in_middle.yaml')
    self.assertFalse(partitioner.should_flatten())
    self.assertEqual(partitioner.get_num_partitions(), 5)
    for i in range(partitioner.get_num_partitions()):
      self.assertTrue(partitioner.should_keep_partition(i))

    # 'chr1:0-1,000,000'
    self.assertEqual(partitioner.get_partition('chr1', 0), 0)
    self.assertEqual(partitioner.get_partition('chr1', 999999), 0)
    # 'chr1:1,000,000-2,000,000'
    self.assertEqual(partitioner.get_partition('chr1', 1000000), 2)
    self.assertEqual(partitioner.get_partition('chr1', 1999999), 2)
    # 'chr2' OR 'ch2' OR 'c2' OR '2'
    self.assertEqual(partitioner.get_partition('chr2', 0), 3)
    self.assertEqual(partitioner.get_partition('chr2', 999999999000), 3)
    # '3:500,000-1,000,000'
    self.assertEqual(partitioner.get_partition('3', 500000), 4)
    self.assertEqual(partitioner.get_partition('3', 999999), 4)

    # All the followings are assigned to residual partition.
    self.assertEqual(partitioner.get_partition('chr1', 2000000), 1)
    self.assertEqual(partitioner.get_partition('chr1', 999999999), 1)

    self.assertEqual(partitioner.get_partition('3', 0), 1)
    self.assertEqual(partitioner.get_partition('3', 499999), 1)
    self.assertEqual(partitioner.get_partition('3', 1000000), 1)

    self.assertEqual(partitioner.get_partition('ch2', 0), 1)
    self.assertEqual(partitioner.get_partition('c2', 0), 1)
    self.assertEqual(partitioner.get_partition('2', 0), 1)

    self.assertEqual(partitioner.get_partition('c4', 0), 1)
    self.assertEqual(partitioner.get_partition('cr5', 0), 1)
    self.assertEqual(partitioner.get_partition('chr6', 0), 1)

  def test_config_residual_partition_absent(self):
    partitioner = variant_partition.VariantPartition(
        'gcp_variant_transforms/testing/data/partition_configs/'
        'residual_missing.yaml')
    self.assertFalse(partitioner.should_flatten())
    self.assertEqual(partitioner.get_num_partitions(), 5)
    # All partitions excpet the last one (dummy residual) should be kept.
    for i in range(partitioner.get_num_partitions() - 1):
      self.assertTrue(partitioner.should_keep_partition(i))
    self.assertFalse(partitioner.should_keep_partition(5 - 1))

    # 'chr1:0-1,000,000'
    self.assertEqual(partitioner.get_partition('chr1', 0), 0)
    self.assertEqual(partitioner.get_partition('chr1', 999999), 0)
    # 'chr1:1,000,000-2,000,000'
    self.assertEqual(partitioner.get_partition('chr1', 1000000), 1)
    self.assertEqual(partitioner.get_partition('chr1', 1999999), 1)
    # 'chr2' OR 'ch2' OR 'c2' OR '2'
    self.assertEqual(partitioner.get_partition('chr2', 0), 2)
    self.assertEqual(partitioner.get_partition('chr2', 999999999000), 2)
    # '3:500,000-1,000,000'
    self.assertEqual(partitioner.get_partition('3', 500000), 3)
    self.assertEqual(partitioner.get_partition('3', 999999), 3)

    # All the followings are assigned to residual partition.
    self.assertEqual(partitioner.get_partition('chr1', 2000000), 4)
    self.assertEqual(partitioner.get_partition('chr1', 999999999), 4)

    self.assertEqual(partitioner.get_partition('3', 0), 4)
    self.assertEqual(partitioner.get_partition('3', 499999), 4)
    self.assertEqual(partitioner.get_partition('3', 1000000), 4)

    self.assertEqual(partitioner.get_partition('ch2', 0), 4)
    self.assertEqual(partitioner.get_partition('c2', 0), 4)
    self.assertEqual(partitioner.get_partition('2', 0), 4)

    self.assertEqual(partitioner.get_partition('c4', 0), 4)
    self.assertEqual(partitioner.get_partition('cr5', 0), 4)
    self.assertEqual(partitioner.get_partition('chr6', 0), 4)

  def test_config_failed_missing_region(self):
    tempdir = temp_dir.TempDir()
    missing_region = [
        '-  partition:',
        '     partition_name: "chr01_part1"',
        '     regions:',
        '       - "chr1:0-1,000,000"',
        '-  partition:',
        '     partition_name: "all_remaining"',
        '     regions:',
        '       - "residual"',
        '-  partition:',
        '     partition_name: "missing_region"',
        '     regions:',
    ]
    with self.assertRaisesRegexp(
        ValueError,
        'Each partition must have at least one region.'):
      _ = variant_partition.VariantPartition(
          tempdir.create_temp_file(suffix='.yaml',
                                   lines='\n'.join(missing_region)))

  def test_config_failed_missing_partition_name(self):
    tempdir = temp_dir.TempDir()
    missing_par_name = [
        '-  partition:',
        '     regions:',
        '       - "chr1:0-1,000,000"',
    ]
    with self.assertRaisesRegexp(
        ValueError,
        'Each partition must have partition_name field.'):
      _ = variant_partition.VariantPartition(
          tempdir.create_temp_file(suffix='.yaml',
                                   lines='\n'.join(missing_par_name)))
    empty_par_name = [
        '-  partition:',
        '     partition_name: "          "',
        '     regions:',
        '       - "chr1:0-1,000,000"',
    ]
    with self.assertRaisesRegexp(
        ValueError,
        'Partition name can not be empty string.'):
      _ = variant_partition.VariantPartition(
          tempdir.create_temp_file(suffix='.yaml',
                                   lines='\n'.join(empty_par_name)))

  def test_config_failed_duplicate_residual_partition(self):
    tempdir = temp_dir.TempDir()
    duplicate_residual = [
        '-  partition:',
        '     partition_name: "all_remaining"',
        '     regions:',
        '       - "residual"',
        '-  partition:',
        '     partition_name: "chr01"',
        '     regions:',
        '       - "chr1"',
        '-  partition:',
        '     partition_name: "all_remaining_2"',
        '     regions:',
        '       - "residual"',
    ]
    with self.assertRaisesRegexp(
        ValueError,
        'There must be only one residual partition.'):
      _ = variant_partition.VariantPartition(
          tempdir.create_temp_file(suffix='.yaml',
                                   lines='\n'.join(duplicate_residual)))

  def test_config_failed_overlapping_regions(self):
    tempdir = temp_dir.TempDir()
    overlapping_regions = [
        '-  partition:',
        '     partition_name: "chr01_part1"',
        '     regions:',
        '       - "chr1:0-1,000,000"',
        '-  partition:',
        '     partition_name: "chr01_part2_overlapping"',
        '     regions:',
        '       - "chr1:999,999-2,000,000"',
    ]
    with self.assertRaisesRegexp(
        ValueError, 'Cannot add overlapping region *'):
      _ = variant_partition.VariantPartition(
          tempdir.create_temp_file(suffix='.yaml',
                                   lines='\n'.join(overlapping_regions)))

    full_and_partial = [
        '-  partition:',
        '     partition_name: "chr01_full"',
        '     regions:',
        '       - "chr1"',
        '-  partition:',
        '     partition_name: "chr01_part_overlapping"',
        '     regions:',
        '       - "chr1:1,000,000-2,000,000"',
    ]
    with self.assertRaisesRegexp(
        ValueError, 'Cannot add overlapping region *'):
      _ = variant_partition.VariantPartition(
          tempdir.create_temp_file(suffix='.yaml',
                                   lines='\n'.join(full_and_partial)))

    partial_and_full = [
        '-  partition:',
        '     partition_name: "chr01_part"',
        '     regions:',
        '       - "chr1:1,000,000-2,000,000"',
        '-  partition:',
        '     partition_name: "chr01_full_overlapping"',
        '     regions:',
        '       - "chr1"',
    ]
    with self.assertRaisesRegexp(
        ValueError, 'Cannot add overlapping region *'):
      _ = variant_partition.VariantPartition(
          tempdir.create_temp_file(suffix='.yaml',
                                   lines='\n'.join(partial_and_full)))

    full_and_full = [
        '-  partition:',
        '     partition_name: "chr01_full"',
        '     regions:',
        '       - "chr1"',
        '-  partition:',
        '     partition_name: "chr02_part"',
        '     regions:',
        '       - "chr2:1,000,000-2,000,000"',
        '-  partition:',
        '     partition_name: "chr01_full_redundant"',
        '     regions:',
        '       - "chr1"',
    ]
    with self.assertRaisesRegexp(
        ValueError, 'Cannot add overlapping region *'):
      _ = variant_partition.VariantPartition(
          tempdir.create_temp_file(suffix='.yaml',
                                   lines='\n'.join(full_and_full)))

  def test_config_failed_duplicate_table_name(self):
    tempdir = temp_dir.TempDir()
    dup_table_name = [
        '-  partition:',
        '     partition_name: "duplicate_name"',
        '     regions:',
        '       - "chr1:0-1,000,000"',
        '-  partition:',
        '     partition_name: "all_remaining"',
        '     regions:',
        '       - "residual"',
        '-  partition:',
        '     partition_name: "duplicate_name"',
        '     regions:',
        '       - "chr1:1,000,000-2,000,000"',
    ]
    with self.assertRaisesRegexp(
        ValueError,
        'Partition names must be unique *'):
      _ = variant_partition.VariantPartition(
          tempdir.create_temp_file(suffix='.yaml',
                                   lines='\n'.join(dup_table_name)))


  def test_config_failed_missing_fields(self):
    tempdir = temp_dir.TempDir()
    missing_output_table = [
        '-  missing___output_table:',
        '     table_name_suffix: "chr1"',
        '     CHROM_values:',
        '       - "chr1"',
        '       - "1"',
        '     total_base_pairs: 249240615'
    ]
    with self.assertRaisesRegexp(
        ValueError,
        'Wrong yaml file format, output_table field missing.'):
      _ = variant_partition._ConfigParser(
        tempdir.create_temp_file(suffix='.yaml',
                                 lines='\n'.join(missing_output_table)))

    missing_table_name_suffix = [
        '-  output_table:',
        '     CHROM_values:',
        '       - "chr1"',
        '       - "1"',
        '     total_base_pairs: 249240615'
    ]
    with self.assertRaisesRegexp(
        ValueError,
        'Wrong yaml file format, table_name_suffix field missing.'):
      _ = variant_partition._ConfigParser(
        tempdir.create_temp_file(suffix='.yaml',
                                 lines='\n'.join(missing_table_name_suffix)))

    missing_chrom_values = [
      '-  output_table:',
      '     table_name_suffix: "chr1"',
      '     total_base_pairs: 249240615'
    ]
    with self.assertRaisesRegexp(
        ValueError,
        'Wrong yaml file format, CHROM_values field missing.'):
      _ = variant_partition._ConfigParser(
        tempdir.create_temp_file(suffix='.yaml',
                                 lines='\n'.join(missing_chrom_values)))

    missing_filters = [
      '-  output_table:',
      '     table_name_suffix: "chr1"',
      '     CHROM_values:',
      '     total_base_pairs: 249240615'
    ]
    with self.assertRaisesRegexp(
        ValueError,
        'Wrong yaml file format, CHROM_values field missing.'):
      _ = variant_partition._ConfigParser(
        tempdir.create_temp_file(suffix='.yaml',
                                 lines='\n'.join(missing_filters)))

    missing_total_base_pairs = [
      '-  output_table:',
      '     table_name_suffix: "chr1"',
      '     CHROM_values:',
      '       - "chr1"',
      '       - "1"',
    ]
    with self.assertRaisesRegexp(
        ValueError,
        'Wrong yaml file format, total_base_pairs field missing.'):
      _ = variant_partition._ConfigParser(
          tempdir.create_temp_file(suffix='.yaml',
                                   lines='\n'.join(missing_total_base_pairs)))


  def test_config_failed_wrong_fields(self):
    tempdir = temp_dir.TempDir()
    empty_suffix = [
      '-  output_table:',
      '     table_name_suffix: " "',
      '     CHROM_values:',
      '       - "chr1"',
      '       - "1"',
      '     total_base_pairs: 249240615'
    ]
    with self.assertRaisesRegexp(
        ValueError,
        'table_name_suffix can not be empty string.'):
      _ = variant_partition._ConfigParser(
        tempdir.create_temp_file(suffix='.yaml',
                                 lines='\n'.join(empty_suffix)))
    tempdir = temp_dir.TempDir()

    wrong_table_name = [
      '-  output_table:',
      '     table_name_suffix: "chr#"',
      '     CHROM_values:',
      '       - "chr1"',
      '       - "1"',
      '     total_base_pairs: 249240615'
    ]
    with self.assertRaisesRegexp(
        ValueError, 'BigQuery table name can only contain letters *'):
      _ = variant_partition._ConfigParser(
        tempdir.create_temp_file(suffix='.yaml',
                                 lines='\n'.join(wrong_table_name)))
    tempdir = temp_dir.TempDir()

    duplicate_suffix = [
      '-  output_table:',
      '     table_name_suffix: "chr1"',
      '     CHROM_values:',
      '       - "chr1"',
      '     total_base_pairs: 249240615',
      '-  output_table:',
      '     table_name_suffix: "chr1"',
      '     CHROM_values:',
      '       - "chr2"',
      '     total_base_pairs: 249240615'
    ]
    with self.assertRaisesRegexp(
        ValueError,
        'Table name suffixes must be unique, chr1 is duplicated.'):
      _ = variant_partition._ConfigParser(
        tempdir.create_temp_file(suffix='.yaml',
                                 lines='\n'.join(duplicate_suffix)))
    tempdir = temp_dir.TempDir()

    empty_chrom_value = [
      '-  output_table:',
      '     table_name_suffix: "chr1"',
      '     CHROM_values:',
      '       - "chr1"',
      '       - " "',
      '     total_base_pairs: 249240615'
    ]
    with self.assertRaisesRegexp(
        ValueError,
        'CHROM_value can not be empty string.'):
      _ = variant_partition._ConfigParser(
        tempdir.create_temp_file(suffix='.yaml',
                                 lines='\n'.join(empty_chrom_value)))
    tempdir = temp_dir.TempDir()

    duplicate_chrom_value1 = [
      '-  output_table:',
      '     table_name_suffix: "chr1"',
      '     CHROM_values:',
      '       - "dup_value"',
      '       - "dup_value"',
      '     total_base_pairs: 249240615'
    ]
    with self.assertRaisesRegexp(
        ValueError,
        'chrom_values must be unique in config file: dup_value .'):
      _ = variant_partition._ConfigParser(
        tempdir.create_temp_file(suffix='.yaml',
                                 lines='\n'.join(duplicate_chrom_value1)))

    duplicate_chrom_value2 = [
      '-  output_table:',
      '     table_name_suffix: "chr1"',
      '     CHROM_values:',
      '       - "dup_value"',
      '     total_base_pairs: 249240615',
      '-  output_table:',
      '     table_name_suffix: "chr2"',
      '     CHROM_values:',
      '       - "dup_value"',
      '     total_base_pairs: 249240615'
    ]
    with self.assertRaisesRegexp(
        ValueError,
        'chrom_values must be unique in config file: dup_value .'):
      _ = variant_partition._ConfigParser(
        tempdir.create_temp_file(suffix='.yaml',
                                 lines='\n'.join(duplicate_chrom_value2)))

    duplicate_residual = [
      '-  output_table:',
      '     table_name_suffix: "residual1"',
      '     CHROM_values:',
      '       - "residual"',
      '     total_base_pairs: 249240615',
      '-  output_table:',
      '     table_name_suffix: "residual2"',
      '     CHROM_values:',
      '       - "residual"',
      '     total_base_pairs: 249240615'
    ]
    with self.assertRaisesRegexp(
        ValueError,
        'There can be only one residual output table.'):
      _ = variant_partition._ConfigParser(
        tempdir.create_temp_file(suffix='.yaml',
                                 lines='\n'.join(duplicate_residual)))

    not_int_total_base_pairs = [
      '-  output_table:',
      '     table_name_suffix: "chr1"',
      '     CHROM_values:',
      '       - "chr1"',
      '       - "1"',
      '     total_base_pairs: "not int"'
    ]
    with self.assertRaisesRegexp(
        ValueError,
        'Each output table needs an int total_base_pairs > 0.'):
      _ = variant_partition._ConfigParser(
        tempdir.create_temp_file(suffix='.yaml',
                                 lines='\n'.join(not_int_total_base_pairs)))

    not_pos_total_base_pairs = [
      '-  output_table:',
      '     table_name_suffix: "chr1"',
      '     CHROM_values:',
      '       - "chr1"',
      '       - "1"',
      '     total_base_pairs: -10'
    ]
    with self.assertRaisesRegexp(
        ValueError,
        'Each output table needs an int total_base_pairs > 0.'):
      _ = variant_partition._ConfigParser(
        tempdir.create_temp_file(suffix='.yaml',
                                 lines='\n'.join(not_pos_total_base_pairs)))




