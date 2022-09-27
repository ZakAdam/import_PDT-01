require 'sequel'
require 'dotenv'
#require 'logger'
require 'set'
require 'objspace'
require 'zlib'
require 'json'
load 'create_tables.rb'
load 'create_author_files.rb'
#load 'create_tweet_files.rb'

Dotenv.load

DATABASE = Sequel.connect(adapter: :postgres,
                    user: ENV.fetch("POSTGRES_USER"),
                    password: ENV.fetch("POSTGRES_PASSWORD"),
                    host: ENV.fetch("POSTGRES_HOST"),
                    port: ENV.fetch("POSTGRES_PORT"),
                    database: ENV.fetch("POSTGRES_DB"),
                    max_connections: 10)
                          #logger: Logger.new('log/db.log'))

create_tables(DATABASE)

#load 'test.rb'
#test_upsert(DATABASE)


#create_smaller_author_files

#=begin
filepath = '/home/adam/Downloads/authors.jsonl.gz'
batch_size = 100000

array_of_authors = []

#author_ids = DATABASE[:authors].all.map{|e| "#{e[:id]}"}.to_set
author_ids = DATABASE[:authors].select(:id).map{|e| "#{e[:id]}"}.to_set
=begin

puts Time.now
Zlib::GzipReader.open(filepath) do |file|
  file.lazy.each_slice(batch_size) do |lines|
    lines.each do |line|
      parsed_line = JSON.parse(line.gsub('\u0000', ''))


      array_of_authors << {id: parsed_line['id'],
                           name: parsed_line['name'],
                           username: parsed_line['username'],
                           description: parsed_line['description'],
                           followers_count: parsed_line.dig('public_metrics', 'followers_count'),
                           following_count: parsed_line.dig('public_metrics', 'following_count'),
                           tweet_count: parsed_line.dig('public_metrics', 'tweet_count'),
                           listed_count: parsed_line.dig('public_metrics', 'listed_count')}
    end
    DATABASE[:authors].insert_conflict.multi_insert(array_of_authors)
    array_of_authors = []
  end
end
puts Time.now

=end

puts Time.now
filepath = '/home/adam/Downloads/conversations.jsonl.gz'
array_of_conversations = []

Zlib::GzipReader.open(filepath) do |file|
  file.lazy.each_slice(batch_size) do |lines|
    lines.each do |line|
      parsed_line = JSON.parse(line.gsub('\u0000', ''))

      array_of_conversations << {id: parsed_line['id'],
                           author_id: author_ids.include?(parsed_line['author_id']) ? parsed_line['author_id'] : nil,
                           content: parsed_line['text'],
                           possible_sensitive: parsed_line['possibly_sensitive'],
                           language: parsed_line['lang'],
                           source: parsed_line['source'],
                           retweet_count: parsed_line.dig('public_metrics', 'retweet_count'),
                           reply_count: parsed_line.dig('public_metrics', 'reply_count'),
                           like_count: parsed_line.dig('public_metrics', 'like_count'),
                           quote_count: parsed_line.dig('public_metrics', 'quote_count'),
                           created_at: parsed_line['created_at']}
    end
    DATABASE[:conversations].insert_conflict.multi_insert(array_of_conversations)
    array_of_conversations = []
  end
end
puts Time.now

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
