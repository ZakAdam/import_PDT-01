require 'sequel'
require 'dotenv'
require 'logger'
require 'objspace'
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
                    max_connections: 10,
                    logger: Logger.new('log/db.log'))

create_tables(DATABASE)

load 'test.rb'
#test_upsert(DATABASE)
exit

#create_smaller_author_files

#=begin
array_of_authors = []

i = 0

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

puts "lol"
