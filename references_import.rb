require 'sequel'
require 'zlib'
require 'json'
require 'csv'

# Method that imports conversation references
# csv_array - array where csv times are stored
# start_time - start of execution
def references_import(csv_array, start_time)
  puts "References start je: #{Time.now}"
  batch_time = Time.now
  batch_size = 100000                                      # size of batch
  batch_number = 0
  filepath = 'conversations.jsonl.gz'
  existing_conversations = DATABASE[:conversations].select(:id).map{|e| e[:id]}.to_set    # load IDs of all conversations to set
  existing_references = Set[]                              # create Set, that holds existing references
  referenced_tweets = []                                   # array of new tweets
  rows = 0

  Zlib::GzipReader.zcat(File.open(filepath)) do |line|      # open stream of unzipped rows
    parsed_line = JSON.parse(line.gsub('\u0000', ''))       # replace null byte, and parse string to Hash
    next if existing_references.include?(parsed_line['id'].to_i)    # if conversation with same ID was already processed, than skip this conversation

    if parsed_line.key?('referenced_tweets')                # if tweet has some references
      parsed_line['referenced_tweets'].each do |tweet|      # for each reference
        next unless existing_conversations.include?(tweet['id'].to_i)     # skip this reference if parent tweet is not in conversation tables
        referenced_tweets << {conversation_id: parsed_line['id'],
                              parent_id: tweet['id'],
                              type: tweet['type']}

        existing_references << parsed_line['id'].to_i       # update existing references
        end
    end

    rows += 1

    if rows % batch_size == 0                               # number of rows is batch size
      # insert array to DB with INSERT INTO command
      DATABASE[:conversation_references].multi_insert(referenced_tweets)
      puts "#{batch_number} - #{referenced_tweets.size}"
      referenced_tweets.clear

      batch_number += 1                                     # increment batch_number

      # add times to csv_array
      csv_array << [Time.now.strftime('%Y-%m-%dT%H:%M%z'), seconds_to_hms(Time.now - start_time), seconds_to_hms(Time.now - batch_time)]

      # upgrade to new batch_time
      batch_time = Time.now
    end
  end

  # import last batch to DB
  DATABASE[:conversation_references].multi_insert(referenced_tweets)

  # add times to csv_array
  csv_array << [Time.now.strftime('%Y-%m-%dT%H:%M%z'), seconds_to_hms(Time.now - start_time), seconds_to_hms(Time.now - batch_time)]

  # clear arrays and free RAM
  referenced_tweets.clear
  existing_conversations.clear
  puts "End je: #{Time.now}"
end
