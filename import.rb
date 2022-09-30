require 'sequel'
require 'dotenv'
require 'logger'
require 'set'
require 'objspace'
require 'zlib'
require 'json'
load 'create_tables.rb'
#load 'create_author_files.rb'
#load 'create_tweet_files.rb'

Dotenv.load

DATABASE = Sequel.connect(adapter: :postgres,
                    user: ENV.fetch("POSTGRES_USER"),
                    password: ENV.fetch("POSTGRES_PASSWORD"),
                    host: ENV.fetch("POSTGRES_HOST"),
                    port: ENV.fetch("POSTGRES_PORT"),
                    database: ENV.fetch("POSTGRES_DB"),
                    max_connections: 10,
                          logger: Logger.new('log/db.log'))

create_tables(DATABASE)

puts "Start je: #{Time.now}"
#load 'test.rb'
#hash_test(DATABASE)
#exit
=begin

batch_size = 100000
batch_number = 0
filepath = 'authors.jsonl.gz'
rows = 0

#author_ids = DATABASE[:authors].select(:id).map{|e| "#{e[:id]}"}.to_set

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
    array_of_authors = []
    batch_number += 1

    #exit if batch_size == 4
  end
end
  DATABASE[:authors].insert_conflict.multi_insert(array_of_authors)
array_of_conversations = []
puts "End je: #{Time.now}"

exit
=end

puts Time.now
batch_size = 100000
batch_number = 0
filepath = 'conversations.jsonl.gz'
array_of_conversations = []
array_of_null_authors = []
existing_conversations = Set[]
referenced_tweets = []
links = []
annotations = []
conversation_hashtags = []
new_hashtags = []
existing_hashtags = {}
rows = 0

author_ids = DATABASE[:authors].select(:id).map{|e| e[:id]}.to_set

Zlib::GzipReader.zcat(File.open(filepath)) do |line|
  parsed_line = JSON.parse(line.gsub('\u0000', ''))
  next if existing_conversations.include?(parsed_line['id'])

  #array_of_null_authors << {id: parsed_line['author_id']} unless author_ids.include?(parsed_line['author_id'])

  unless author_ids.include?(parsed_line['author_id'].to_i)
    array_of_null_authors << {id: parsed_line['author_id']}
    author_ids << parsed_line['author_id'].to_i
  end

  array_of_conversations << {id: parsed_line['id'],
                             author_id: parsed_line['author_id'],
                             #author_id: author_ids.include?(parsed_line['author_id']) ? parsed_line['author_id'] : nil,
                             content: parsed_line['text'],
                             possible_sensitive: parsed_line['possibly_sensitive'],
                             language: parsed_line['lang'],
                             source: parsed_line['source'],
                             retweet_count: parsed_line.dig('public_metrics', 'retweet_count'),
                             reply_count: parsed_line.dig('public_metrics', 'reply_count'),
                             like_count: parsed_line.dig('public_metrics', 'like_count'),
                             quote_count: parsed_line.dig('public_metrics', 'quote_count'),
                             created_at: parsed_line['created_at']}

  if parsed_line.key?('referenced_tweets')
    parsed_line['referenced_tweets'].each do |tweet|
      referenced_tweets << {conversation_id: parsed_line['id'],
                            parent_id: tweet['id'],
                            type: tweet['type']}
      end
  end

  if parsed_line.dig('entities', 'urls')
    parsed_line.dig('entities', 'urls').each do |url|
      links << {conversation_id: parsed_line['id'],
                url: url['expanded_url'],
                title: url['title'],
                description: url['description']}
    end
  end

  if parsed_line.dig('entities', 'annotations')
    parsed_line.dig('entities', 'annotations').each do |annotation|
      annotations << {conversation_id: parsed_line['id'],
                value: annotation['normalized_text'],
                type: annotation['type'],
                probability: annotation['probability']}
    end
  end

  existing_conversations << parsed_line['id']

  rows += 1

  if rows % batch_size == 0
    DATABASE[:authors].insert_conflict(:target=>:id).multi_insert(array_of_null_authors)        # TODO odstran upsert!!!
    DATABASE[:conversations].insert_conflict(:target=>:id).multi_insert(array_of_conversations)
    DATABASE[:links].multi_insert(links)
    #DATABASE[:conversation_references].multi_insert(referenced_tweets)
    DATABASE[:annotations].multi_insert(annotations)
    puts "#{batch_number} - #{array_of_conversations.size}"
    array_of_conversations = []
    array_of_null_authors = []
    links = []
    referenced_tweets = []
    annotations = []
    batch_number += 1

    #exit if batch_size == 4
  end
end


DATABASE[:authors].multi_insert(array_of_null_authors)
DATABASE[:conversations].insert_conflict(:target=>:id).multi_insert(array_of_conversations)
DATABASE[:links].multi_insert(links)
DATABASE[:conversation_references].multi_insert(referenced_tweets)
DATABASE[:annotations].multi_insert(annotations)
puts "#{batch_number} - #{array_of_conversations.size}"
array_of_conversations = []
array_of_null_authors = []
links = []
referenced_tweets = []
annotations = []
batch_number += 1

=begin

puts Time.now
(0..55).each do |number|
  File.open("/home/adam/Downloads/autori_#{number}.jsonl", 'r') do |file|
    file.each_line do |line|
      #puts line.class
      #puts line.encoding # UTF-8
      #puts line if i > 3000 and i < 4000
      #puts "DEBIL: #{line.chars.map(&:ord)}" if line.include?("conductor")  # 92, 117, 48x4
      #puts "DEBIL: #{line.chars.map(&:ord)}" if line.include?('\\u0000')  # 92, 117, 48x4

      #parsed_line = JSON.parse(line.delete('\u0000'))
      parsed_line = JSON.parse(line.gsub('\u0000', ''))


      array_of_authors << {id: parsed_line['id'],
                           name: parsed_line['name'],
                           username: parsed_line['username'],
                           description: parsed_line['description'],
                           followers_count: parsed_line.dig('public_metrics', 'followers_count'),
                           following_count: parsed_line.dig('public_metrics', 'following_count'),
                           tweet_count: parsed_line.dig('public_metrics', 'tweet_count'),
                           listed_count: parsed_line.dig('public_metrics', 'listed_count')}

      #puts ObjectSpace.memsize_of(array_of_authors)
      #i += 1
      #if i == 5
        #lol = DATABASE[:authors].multi_insert(array_of_authors)
        #puts 'Endujem sa :)'
        #puts lol
        #exit
        #end

    end

    DATABASE[:authors].insert_conflict.multi_insert(array_of_authors)
    array_of_authors = []
  end
end
puts Time.now
#=end
=end
puts "lol"


# Start je: 2022-09-30 19:15:41 +0200