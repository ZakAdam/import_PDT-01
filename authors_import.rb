require 'sequel'
require 'zlib'
require 'json'

def authors_import(csv_array, start_time)
  batch_time = Time.now
  batch_size = 100000
  batch_number = 0
  filepath = 'authors.jsonl.gz'
  rows = 0

  array_of_authors = []

  Zlib::GzipReader.zcat(File.open(filepath)) do |line|
    parsed_line = JSON.parse(line.gsub('\u0000', ''))

    array_of_authors << {id: parsed_line['id'],               # 5500125
                         name: parsed_line['name'],
                         username: parsed_line['username'],
                         description: parsed_line['description'],
                         followers_count: parsed_line.dig('public_metrics', 'followers_count'),
                         following_count: parsed_line.dig('public_metrics', 'following_count'),
                         tweet_count: parsed_line.dig('public_metrics', 'tweet_count'),
                         listed_count: parsed_line.dig('public_metrics', 'listed_count')}

    rows += 1

    if rows % batch_size == 0
      DATABASE[:authors].insert_conflict(:target=>:id).multi_insert(array_of_authors)
      puts "#{batch_number} - #{array_of_authors.size}"
      array_of_authors.clear
      batch_number += 1
      csv_array << [Time.now.strftime('%Y-%m-%dT%H:%M%z'), seconds_to_hms(Time.now - start_time), seconds_to_hms(Time.now - batch_time)]
      batch_time = Time.now
      #exit if batch_size == 4
    end
  end
  DATABASE[:authors].insert_conflict.multi_insert(array_of_authors)
  csv_array << [Time.now.strftime('%Y-%m-%dT%H:%M%z'), seconds_to_hms(Time.now - start_time), seconds_to_hms(Time.now - batch_time)]
  array_of_authors.clear
  puts "End je: #{Time.now}"
end
