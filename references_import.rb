require 'sequel'
require 'zlib'
require 'json'
require 'csv'

# runtime = 1h 15m
def references_import(csv_array, start_time)
  puts "References start je: #{Time.now}"
  batch_time = Time.now
  batch_size = 100000
  batch_number = 0
  filepath = 'conversations.jsonl.gz'
  existing_conversations = DATABASE[:conversations].select(:id).map{|e| e[:id]}.to_set
  existing_references = Set[]
  referenced_tweets = []
  rows = 0

  Zlib::GzipReader.zcat(File.open(filepath)) do |line|
    parsed_line = JSON.parse(line.gsub('\u0000', ''))
    #next unless existing_conversations.include?(parsed_line['id'])
    next if existing_references.include?(parsed_line['id'].to_i)

    if parsed_line.key?('referenced_tweets')
      parsed_line['referenced_tweets'].each do |tweet|
        next unless existing_conversations.include?(tweet['id'].to_i)
        referenced_tweets << {conversation_id: parsed_line['id'],
                              parent_id: tweet['id'],
                              type: tweet['type']}

        existing_references << parsed_line['id'].to_i
        end
    end

    rows += 1

    if rows % batch_size == 0
      DATABASE[:conversation_references].multi_insert(referenced_tweets)
      puts "#{batch_number} - #{referenced_tweets.size}"
      referenced_tweets.clear

      batch_number += 1

      csv_array << [Time.now.strftime('%Y-%m-%dT%H:%M%z'), seconds_to_hms(Time.now - start_time), seconds_to_hms(Time.now - batch_time)]
      batch_time = Time.now
      #exit if batch_size == 4
    end
  end
  DATABASE[:conversation_references].multi_insert(referenced_tweets)
  csv_array << [Time.now.strftime('%Y-%m-%dT%H:%M%z'), seconds_to_hms(Time.now - start_time), seconds_to_hms(Time.now - batch_time)]
  referenced_tweets.clear
  existing_conversations.clear
  puts "End je: #{Time.now}"
end
