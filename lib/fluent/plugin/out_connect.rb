require 'fluent/output'
require 'fluent/plugin/kafka_plugin_util'


class Fluent::ConnectOutput < Fluent::Output
  Fluent::Plugin.register_output('connect', self)

  config_param :brokers, :string, :default => 'localhost:9092',
               :desc => <<-DESC
Set brokers directly
<broker1_host>:<broker1_port>, <broker2_host>:<broker2_port>, ...
Note: You can choose either brokers or Zookeeper.
DESC
  config_param :zookeeper, :string, :default => nil,
               :desc => "Set brokers via Zookeeper: <zookeeper_host>:<zookeeper_port>"
  config_param :zookeeper_path, :string, :default => '/brokers/ids',
               :desc => "Path in path for Broker ID.  Default to /brokers/ids"
  config_param :default_topic, :string, :default => nil,
               :desc => "Output topic"
  config_param :default_message_key, :string, :default => nil
  config_param :default_partition_key, :string, :default => nil
  config_param :client_id, :string, :default => 'kafka'
  config_param :output_include_tag, :bool, :default => false
  config_param :output_include_time, :bool, :default => false
  config_param :exclude_partition_key, :bool, :default => false,
               :desc => "Set true to remove parition key from data"
  config_param :exclude_message_key, :bool, :default => false,
               :desc => "Set true to remove message key from data"
  config_param :exclude_topic_key, :bool, :default => false,
               :desc => "Set to true to remove topic name key from data"
  config_param :schema, :string, :default => nil,
               :desc => "For 'avro' format only"
  config_param :flatten, :bool, :default => true,
               :desc => "Set to false to keep record as is"
  # ruby-kafka producer options
  config_param :max_send_retries, :integer, :default => 2,
               :desc => "Number of times to retry sending of messages to a leader."
  config_param :required_acks, :integer, :default => -1,
               :desc => "The number of acks required per request."
  config_param :ack_timeout, :integer, :default => nil,
               :desc => "How long the producer waits for acks."
  config_param :compression_codec, :string, :default => nil,
               :desc => "The codec the producer uses to compress messages."
  # time format
  config_param :time_format, :string, :default => nil


  include Fluent::KafkaPluginUtil::SSLSettings


  attr_accessor :output_data_type
  attr_accessor :field_separator


  unless method_defined?(:log)
    define_methon("log") { $log }
  end

  def initialize
    super
    require 'kafka'
    @kafka = nil
  end


  def refresh_client
    if @zookeeper
      @seed_brokers = []
      z = Zookeeper.new(@zookeeper)
      z.get_children(:path => @zookeeper_path)[:children].each do |id|
        broker = Yajl.load(z.get(:path => @zookeeper_path + "/#{id}")[:data])
        @seed_brokers.push("#{broker['host']}:#{broker['port']}")
      end
      z.close
      log.info "brokers have been refreshed via Zookeeper: #{@seed_brokers}"
    end
    begin
      if @seed_brokers.length > 0
        @kafka = Kafka.new(seed_brokers: @seed_brokers, client_id: @client_id, ssl_ca_cert: read_ssl_file(@ssl_ca_cert),
                           ssl_client_cert: read_ssl_file(@ssl_client_cert), ssl_client_cert_key: read_ssl_file(@ssl_client_cert_key))
        log.info "initialized kafka producer: #{@client_id}"
      else
        log.warn "No brokers found in Zookeeper"
      end
    rescue Exception => e
      log.error e
    end
  end


  def configure(conf)
    super

    if @zookeeper
      require 'zookeeper'
    else
      @seed_brokers = @brokers.match(",").nil? ? [@brokers] : @brokers.split(",")
      log.info "brokers has been set directly: #{@seed_brokers}"
    end

    @f_separator = case @field_separator
                   when /SPACE/i then ' '
                   when /COMMA/i then ','
                   when /SOH/i then "\x01"
                   else "\t"
                   end

    @formatter_proc = setup_formatter(conf)
    @producer_opts = {max_retries: @max_send_retries, required_acks: @required_acks}
    @producer_opts[:ack_timeout] = @ack_timeout if @ack_timeout
    @producer_opts[:compression_codec] = @compression_codec.to_sym if @compression_codec
  end


  def start
    super
    refresh_client
  end

  def shutdown
    super
    @kafka = nil
  end


  def setup_formatter(conf)
    # encode fluentd stream to avro format
    require 'avro'
    require 'json'
    Proc.new { |tag, time, record|
      @hash = nil
      @result = {}
      buffer = StringIO.new
      schema = Avro::Schema.parse(@schema)
      writer = Avro::IO::DatumWriter.new(schema)
      encoder = Avro::IO::BinaryEncoder.new(buffer)
      # magic byte!, needed by the Confluent platform---
      buffer.write [0].pack("c*")
      buffer.write [1].pack("N*")
      #-------------------------------------------------
      # convert our records JSON log to a hash
      log.info "pre record=#{record}"
      record.each do |key, value|
        record[key] = (JSON.parse(value) if key == 'log') || value
      end

      record = (flatten(record) if @flatten) || record
      log.info "records=#{record}"
      writer.write(record, encoder)
      buffer.string
    }
  end

  def flatten(hash = @hash, old_path = [])
    hash.each do |key, value|
      current_path = old_path + [key]

      if value.is_a?(Hash)
        flatten(value, current_path)
      elsif value.is_a?(Array)
        value.each_with_index do |v, idx|
          tmp = {"#{idx}" => v}
          flatten(tmp, current_path)
        end
      else
        @result[current_path.join("_")] = value
      end
    end

    @result
  end

  def emit(tag, es, chain)
    begin
      chain.next

      producer = @kafka.producer(@producer_opts)

      es.each do |time, record|
        if @output_include_time
          if @time_format
            record['time'] = Time.at(time).strftime(@time_format)
          else
            record['time'] = time
          end
        end

        record['tag'] = tag if @output_include_tag
        topic = (@exclude_topic_key ? record.delete('topic') : record['topic']) || @default_topic || tag
        partition_key = (@exclude_partition_key ? record.delete('partition_key') : record['partition_key']) || @default_partition_key
        message_key = (@exclude_message_key ? record.delete('message_key') : record['message_key']) || @default_message_key
        value = @formatter_proc.call(tag, time, record)

        log.on_trace { log.trace("message send to #{topic} with partition_key: #{partition_key}, message_key: #{message_key} and value: #{value}") }

        producer.produce(value, topic: topic, key: message_key, partition_key: partition_key)
      end

      producer.deliver_messages
      producer.shutdown
    rescue Exception => e
      log.warn "Send exception occurred: #{e}"
      producer.shutdown if producer
      refresh_client
      raise e
    end
  end


end
