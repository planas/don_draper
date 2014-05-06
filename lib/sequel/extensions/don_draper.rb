class DonDraper
  attr_reader :db

  def initialize(db)
    unless db.database_type == :postgres
      raise ArgumentError, "Don Draper is only compatible with PostgreSQL, sorry =^("
    end

    @db = db
  end

  def create_trigger(table, spin = 0, length = 10, prefix_length = 2, opts = {})
    trigger_name  = opts[:trigger_name]  || "pgt_dd_#{table}"
    function_name = opts[:function_name] || "pgt_dd_#{table}"
    sequence_name = opts[:sequence_name] || table.to_s.foreign_key + "_seq"

    column_name = db.quote_identifier(opts[:column_name] || 'id')

    # Max value for int is 2147483647, we can't guess
    # if the result will be under that threshold, so
    # just use bigint when length is >= 10
    column_type = length + prefix_length >= 10 ? 'bigint' : 'int'

    if prefix_length > 0
      random_min = ('1' + '0' * (prefix_length - 1)).to_i
      random_max = ('1' + '0' * prefix_length).to_i - random_min
    end

    sql = <<-SQL.gsub(/\n\s+\n/, "\n")

    DECLARE
      draperized_id text;
      #{'random_prefix int;' if prefix_length > 0}
    BEGIN
      draperized_id := draperize(nextval('#{sequence_name}')::text, #{spin}, #{length});
      #{"random_prefix := floor(#{random_max} * random() + #{random_min});" if prefix_length > 0}

      NEW.#{column_name} := #{prefix_length > 0 ? "(random_prefix::text || draperized_id)::#{column_type}" : "draperized_id::#{column_type}"};
      RETURN NEW;
    END;
    SQL

    db.create_function(function_name, sql, :language => :plpgsql, :returns => :trigger, :replace => true)
    db.create_trigger(table, trigger_name, function_name, :events => [:insert], :each_row => true)
  end

  def setup
    if @setup_done
      puts "\nWARNING: You are calling DonDrapper#setup more than once, and you shouldn't"
    end

    create_draperize_function
    create_rotate_array_function
    create_zero_padding_function
    create_swapper_map_function
    create_swap_function
    create_scatter_function

    @setup_done = true
  end

  private

  def create_draperize_function
    draperize = <<-SQL

    var swap = plv8.find_function("_dd__swap");
    var scatter = plv8.find_function("_dd__scatter");
    var zero_padding = plv8.find_function("_dd__zero_padding");

    return scatter(swap(zero_padding(input, length).split(''), spin), spin, length).join('');
    SQL

    db.create_function "draperize", draperize,
      :language => :plv8,
      :args     => [[:text, :input], ['int DEFAULT 0', :spin], ['int DEFAULT 10', :length]],
      :returns  => :text,
      :replace  => true,
      :behavior => :immutable
  end

  def create_rotate_array_function
    sql = <<-SQL

    for(var l = a.length, p = -Math.abs(p), p = (Math.abs(p) >= l && (p %= l), p < 0 && (p += l), p), i, x; p; p = (Math.ceil(l / p) - 1) * p - l + (l = p))
      for(i = l; i > p; x = a[--i], a[i] = a[i - p], a[i - p] = x);
    return a;
    SQL

    db.create_function "_dd__rotate_array", sql,
      :language => :plv8,
      :args     => [[:anyarray, :a, :in], [:int, :p]],
      :returns  => :anyarray,
      :replace  => true,
      :behavior => :immutable
  end

  def create_zero_padding_function
    sql = <<-SQL

    if(input.length < width)
    {
      for(var i = 0, buff = ""; i < width - input.length; i++)
        buff += "0";

      return buff + input;
    }
    else
      return input;
    SQL

    db.create_function "_dd__zero_padding", sql,
      :language => :plv8,
      :args     => [[:text, :input], [:int, :width]],
      :returns  => :text,
      :replace  => true,
      :behavior => :immutable
  end

  def create_swapper_map_function
    sql = <<-SQL

    var array = [0,1,2,3,4,5,6,7,8,9], output = [];
    var rotate_array = plv8.find_function("_dd__rotate_array");

    for(var i = 0; i < 10; i++)
      output[i] = rotate_array(array, index + i ^ spin).pop();

    return output;
    SQL

    db.create_function "_dd__swapper_map", sql,
      :language => :plv8,
      :args     => [[:int, :index], [:int, :spin]],
      :returns  => :text,
      :replace  => true,
      :behavior => :immutable
  end

  def create_swap_function
    sql = <<-SQL

    var output = [];
    var swapper_map = plv8.find_function("_dd__swapper_map");

    for(var i = 0; i < input.length; i++)
      output[i] = swapper_map(i, spin)[parseInt(input[i])];

    return output;
    SQL

    db.create_function "_dd__swap", sql,
      :language => :plv8,
      :args     => [['text[]', :input], [:int, :spin]],
      :returns  => 'text[]',
      :replace  => true,
      :behavior => :immutable
  end

  def create_scatter_function
    sql = <<-SQL

    var sum = 0, output = [];
    var rotate_array = plv8.find_function("_dd__rotate_array");

    for(var i = 0; i < input.length; i++)
      sum += parseInt(input[i]);

    for(var k = 0; k < length; k++)
      output[k] = rotate_array(input, spin ^ sum).pop();

    return output;
    SQL

    db.create_function "_dd__scatter", sql,
      :language => :plv8,
      :args     => [['text[]', :input], [:int, :spin], [:int, :length]],
      :returns  => 'text[]',
      :replace  => true,
      :behavior => :immutable
  end
end

Sequel::Database.register_extension :don_draper do |db|
  db.instance_eval do
    @don_draper = DonDraper.new(self)

    def don_draper
      @don_draper
    end
  end
end
