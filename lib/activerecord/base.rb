require 'bulk_insert/worker'

module ActiveRecord
  class Base
    def self.inherited(child_class)
      super
      class << child_class
        def bulk_insert(*columns, set_size:500)
          columns = self.column_names - %w(id) if columns.empty?
          worker = BulkInsert::Worker.new(connection, table_name, columns, set_size)

          if block_given?
            transaction do
              yield worker
              worker.save!
            end
            self
          else
            worker
          end
        end

        #########################################################################
        # helper methods for preparing the columns before a call to :bulk_insert
        #########################################################################

        def default_bulk_columns
          self.column_names - %w(id)
        end

        def bulk_columns_without_timestamps
          default_bulk_columns - %w(created_at updated_at)
        end
      end
    end
  end
end