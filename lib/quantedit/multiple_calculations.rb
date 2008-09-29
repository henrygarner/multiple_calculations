ActiveRecord::Calculations::ClassMethods.module_eval do
  CALCULATIONS_OPERATIONS = [:max, :min, :avg, :sum, :count]
  
  attr_reader :calculations
  
  def calculations_hash
    @calculations_hash ||= calculations.inject(HashWithIndifferentAccess.new) { |hash, calculation| hash[calculation.name] = calculation; hash }
  end
  
  private
  
  # we want to match 
  # calculate(:operation, :column. options={})
  # calculate([:operation_column1, :operation_distinct_column2], options={})
  # calculate(:operation_column1, :operation_ditinct_column2, options={})
  def calculate_with_multiple_columns(*args)
    return calculate_without_multiple_columns(*args) if CALCULATIONS_OPERATIONS.include? args.first
    options         = (args.last.is_a? Hash) ? args.pop : {}
    column_aliases  = args.flatten
    
    #if column_aliases.size == 1
    #  operation, distinct, column_name = tokenize_calculation_column_alias column_aliases.first
    #  return calculate_without_multiple_columns(operation, column_name, options.merge(:distinct => !distinct.empty?))
    #end
    group_columns       = options[:group].to_s.split(',').collect(&:strip)
    #group_headers       = group_columns.collect { |column| columns_hash[column].human_name }
    calculations        = column_aliases.collect { |column| calculations_hash[column] }
    #header_record       = group_headers + calculations.collect(&:human_name)

    select          = (group_columns + calculations.collect(&:to_sql)) * ', '
    #[header_record] + find_without_instantiation(:all, options.merge(:select => select))
    find(:all, options.merge(:select => select))
  end
  alias_method_chain :calculate, :multiple_columns
  
  def aggregate(column, options)
    (@calculations ||= []) << Calculation.new("#{options[:with]}_#{column}", options[:as])
  end
  
  class Calculation
    CALCULATIONS_OPERATIONS = [:max, :min, :avg, :sum, :count]
    
    attr_reader :name, :column_name, :operation, :distinct, :human_name
    
    def initialize(name, human_name=nil)
      match, operation, is_distinct, column_name = name.to_s.match(/^(#{ CALCULATIONS_OPERATIONS * '|' })(_distinct)?_(\w+)$/).to_a
      @name, @operation, @column_name, @distinct, @human_name = name, operation.upcase, column_name, (is_distinct ? true : false), (human_name || name).to_s
    end
    
    def to_sql(&block)
      block_given? ? yield(operation, distinct, column_name) : "#{operation.to_s.upcase}(#{distinct}#{column_name}) as #{name}"
    end
    
    def distinct
      @distinct ? 'DISTINCT ' : nil
    end
    
    def distinct?
      distinct ? true : false
    end
  end
  
end