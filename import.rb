require 'sequel'
require 'dotenv'
load 'create_tables.rb'
load 'create_author_files.rb'

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
                    #create_smaller_author_files

                    #=begin

i = 0
array_of_authors = []
File.open("/home/adam/Downloads/autori_2.jsonl", 'r') do |file|
  file.each_line do |line|
    puts line.class
    parsed_line = JSON.parse(line)

    array_of_authors << {name: parsed_line['name'],
                         username: parsed_line['username'],
                         description: parsed_line['description'],
                         followers_count: parsed_line['public_metrics']['followers_count'],
                         following_count: parsed_line['public_metrics']['following_count'],
                         tweet_count: parsed_line['public_metrics']['tweet_count'],
                         listed_count: parsed_line['public_metrics']['listed_count']}

    i += 1
    if i == 5
      lol = DATABASE[:authors].multi_insert(array_of_authors)
      puts 'Endujem sa :)'
      puts lol
      exit
    end
  end
end
                    #=end

puts "lol"
