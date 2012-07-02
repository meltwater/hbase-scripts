require 'socket'
require 'optparse'
require 'java'

import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.HRegionInfo
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.util.FSUtils
import org.apache.hadoop.hbase.util.Writables
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.commons.logging.LogFactory

opts = {
  :debug => false,
  :port  => 2000,
  :interval => 59,
}

opt = OptionParser.new do |o|

  o.banner = "Usage: #{$0} -m (client|server) [options]"

  o.on( "-m", "--mode MODE", [ :client, :server ], "Execution mode: client or server. If mode is 'client', you must specify a server (-s host), and a timestamp (-t UNIX epoach) to start from." ) do |mode|
    opts[:mode] = mode
  end

  o.on( "-d", "--debug", "Run in debug mode, don't write to HBase in client mode" ) do |d|
    opts[:debug] = d
  end

  o.on_tail("-h", "--help", "Show this message") do
    puts opt
    exit
  end

  o.on( "-s", "--server HOST", "hostname of server to receive data from" ) do |s|
    opts[:server] = s
  end

  o.on( "-p", "--port PORT", Integer, "port on server to listen on (server mode) or receive data from (client mode)" ) do |p|
    opts[:port] = p
  end

  o.on( "-t", "--timestamp TIMESTAMP", Integer, "opts[:timestamp] to begin streaming from" ) do |t|
    opts[:timestamp] = t
  end

  o.on( "-i", "--interval INTERVAL", Integer, "seconds from (and including) opts[:timestamp] to stream data for. Usually, this would not be % 10 == 0 (think about it)." ) do |i|
    opts[:interval] = i
  end

end

opt.parse!

if opts[:mode].nil? ||
   ( opts[:mode].to_s == "client" && opts[:server].nil? )
  puts opt
  exit 
end

class HBaseStreamProtocol
  @@VERSION = 1
  @@PROTOCOL = %w{
    rowkey
    fm_contents:bodyText 
    fm_contents:title
    fm_input_info:author
    fm_input_info:baseurl
    fm_input_info:campId
    fm_input_info:createdDate
    fm_input_info:insertedDate
    fm_input_info:languageCode
    fm_input_info:mediaType
    fm_input_info:sourceCode
    fm_input_info:sourceId
    fm_input_info:url
  }

  @@FIELDS_WITH_FAMILY = []
  # static
  @@PROTOCOL[1..-1].each do |key|
    column, family = key.split /:/ 
    @@FIELDS_WITH_FAMILY << [column.to_java_bytes, family.to_java_bytes ]
  end

  # returns true if the version passed in matches the version of this protocol
  def self.is_compatible? version
    !version.nil? && @@VERSION == version.to_i
  end

  # returns an array of fieldnames, those being strings.
  def self.fields
    @@PROTOCOL
  end

  # returns an array of arrays ( [ family, column ] ), those being java byte[]
  def self.fields_with_family
    @@FIELDS_WITH_FAMILY
  end

end

class HBaseStreamClient

  def initialize opts
    @opts = opts
  end

  def receive
    start_time = Time.now
    
    config = HBaseConfiguration.new
    config.set 'fs.default.name', config.get(HConstants::HBASE_DIR)
    
    table = HTable.new config, 'buzz_data'.to_java_bytes
    # table.setAutoFlush true
    
    socket = TCPSocket.new @opts[:server], @opts[:port]
    # block until disconnected by server
    
    socket.puts @opts[:timestamp].to_s
    socket.puts( @opts[:timestamp] + @opts[:interval] ).to_s
    dupes = 0
    news = 0
    
    while true
      obj = {}
      rowKey = socket.gets
      break if rowKey.nil?
      # print "rowKey => #{rowKey}"
      rowKey.chomp!
      put = Put.new rowKey.to_java_bytes
      HBaseStreamProtocol.fields_with_family.each do |key|
        val = socket.gets
        val.chomp!
        val.gsub!( /<DEADBEEF>>/, "\n" )
        # print "#{key} => #{val}"
        put.add key[0], key[1], val.to_java_bytes
      end
      if @opts[:debug]
        get = Get.new rowKey.to_java_bytes
        if table.exists get
          puts "exists: #{rowKey}"
          dupes = dupes + 1
        else
          puts "new: #{rowKey}"
          news = news + 1
        end
      else
        table.put put
        news = news + 1
      end
    end

    if @opts[:debug]
      puts "client disconnected. Added #{news} items, already had #{dupes} items. Took: #{Time.now - start_time} seconds."
    else
      table.flushCommits
      table.close
      puts "client disconnected. Added #{news} items. Took: #{Time.now - start_time} seconds."
    end
  end
end

class HBaseStreamServer
  def initialize opts
    @opts = opts
  end

  def connect_table
    config = HBaseConfiguration.create
    config.set 'fs.default.name', config.get(HConstants::HBASE_DIR)
    
    HTable.new config, 'buzz_data'.to_java_bytes
  end

  def listen

    server = TCPServer.new @opts[:port]

    table = connect_table

    while client = server.accept
      begin
        puts "client connected."

        timestamp_start = client.gets
        timestamp_end = client.gets

        puts "asking for range #{timestamp_start} to #{timestamp_end}"

        start = sprintf "%s00000000", timestamp_start
        _end  = sprintf "%s99zzzzzz", timestamp_end

        scan = Scan.new start.to_java_bytes, _end.to_java_bytes
        HBaseStreamProtocol.fields.each do |key|
          scan.addColumn key.to_java_bytes
        end

        scanner = table.getScanner scan
        count = 0
        while row = scanner.next
          client.puts String.from_java_bytes row.getRow
          HBaseStreamProtocol.fields_with_family.each do |key|
              value = row.getValue( *key )
              out = String.from_java_bytes( value ).gsub( /\n/, "<DEADBEEF>>" ) rescue ""
              client.puts out
          end
          count = count + 1
        end
        client.close
        puts "disconnecting client. streamed #{count} objects."
      rescue Errno::ECONNRESET
        puts "client disconnected."
      end
    end # loop
  end # listen

end

if opts[:mode].to_s == "client"
  client = HBaseStreamClient.new opts
  client.receive
else
  server = HBaseStreamServer.new opts
  server.listen
end
