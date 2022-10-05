require 'sequel'
require 'zlib'
require 'json'

# method that imports authors to table
# csv_array - array where csv times are stored
# start_time - start of execution
def authors_import(csv_array, start_time)
  batch_time = Time.now
  batch_size = 100000                     # batch size
  batch_number = 0
  filepath = 'authors.jsonl.gz'
  rows = 0

  array_of_authors = []                   # array, which will hold new authors

  Zlib::GzipReader.zcat(File.open(filepath)) do |line|        # open stream of unzipped rows
    parsed_line = JSON.parse(line.gsub('\u0000', ''))         # replace null byte and parse string to Hash

    array_of_authors << {id: parsed_line['id'],               # add row to array
                         name: parsed_line['name'],
                         username: parsed_line['username'],
                         description: parsed_line['description'],
                         followers_count: parsed_line.dig('public_metrics', 'followers_count'),
                         following_count: parsed_line.dig('public_metrics', 'following_count'),
                         tweet_count: parsed_line.dig('public_metrics', 'tweet_count'),
                         listed_count: parsed_line.dig('public_metrics', 'listed_count')}

    rows += 1

    if rows % batch_size == 0             # if rows are in batch size
      # insert array to DB with INSERT INTO command
      DATABASE[:authors].insert_conflict(:target=>:id).multi_insert(array_of_authors)
      puts "#{batch_number} - #{array_of_authors.size}"
      array_of_authors.clear              # free array
      batch_number += 1                   # increment batch_number

      # add new times to csv_array
      csv_array << [Time.now.strftime('%Y-%m-%dT%H:%M%z'), seconds_to_hms(Time.now - start_time), seconds_to_hms(Time.now - batch_time)]

      # update new batch_time
      batch_time = Time.now
    end
  end

  # insert array to DB with INSERT INTO command
  DATABASE[:authors].insert_conflict.multi_insert(array_of_authors)

  # add new times to csv_array
  csv_array << [Time.now.strftime('%Y-%m-%dT%H:%M%z'), seconds_to_hms(Time.now - start_time), seconds_to_hms(Time.now - batch_time)]

  # clear array and free RAM
  array_of_authors.clear
  puts "End je: #{Time.now}"
end
