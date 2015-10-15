require 'test_helper'

class BulkInsertTest < ActiveSupport::TestCase
  test "bulk_insert without block should return worker" do
    result = Testing.bulk_insert
    assert_kind_of BulkInsert::Worker, result
  end

  test "bulk_insert with block should yield worker" do
    result = nil
    Testing.bulk_insert { |worker| result = worker }
    assert_kind_of BulkInsert::Worker, result
  end

  test "bulk_insert with block should save automatically" do
    assert_difference "Testing.count", 1 do
      Testing.bulk_insert do |worker|
        worker.add greeting: "Hello"
      end
    end
  end

  test "default_bulk_columns should return all columns without id" do
    default_columns = %w(greeting age happy created_at updated_at)

    assert_equal Testing.default_bulk_columns, default_columns
  end

  test "bulk_columns_without_timestamps should return all columns without id and timestamps" do
    default_columns = %w(greeting age happy)

    assert_equal Testing.bulk_columns_without_timestamps, default_columns
  end

end
