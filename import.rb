require 'sequel'
require 'dotenv'
require 'set'
require 'zlib'
require 'json'
require 'csv'
load 'create_tables.rb'
load 'authors_import.rb'
load 'import_references.rb'

Dotenv.load

DATABASE = Sequel.connect(adapter: :postgres,
                    user: ENV.fetch("POSTGRES_USER"),
                    password: ENV.fetch("POSTGRES_PASSWORD"),
                    host: ENV.fetch("POSTGRES_HOST"),
                    port: ENV.fetch("POSTGRES_PORT"),
                    database: ENV.fetch("POSTGRES_DB"),
                    max_connections: 10)
                          #logger: Logger.new('log/db.log'))

def seconds_to_hms(sec)
  # https://stackoverflow.com/questions/28908214/converting-seconds-into-hours-only-using-ruby-in-built-function-except-the-day
  "%02d:%02d" % [sec / 60, sec % 60]
end

start_time = Time.now
csv_array = []

create_tables(DATABASE)
                          #authors_import(csv_array ,start_time)

puts "Start je: #{start_time}"

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

=begin
puts Time.now
batch_size = 100000
batch_number = 0
filepath = 'conversations.jsonl.gz'
array_of_conversations = []
array_of_null_authors = []
existing_conversations = Set[]
links = []
annotations = []
context_annotations = []
context_domain = []
existing_domains = Set[]
context_entity = []
existing_entities = Set[]

conversation_hashtags = []
new_hashtags = []
existing_hashtags = {}
last_hashtag_id = 1
rows = 0

batch_time = Time.now

author_ids = DATABASE[:authors].select(:id).map{|e| e[:id]}.to_set

Zlib::GzipReader.zcat(File.open(filepath)) do |line|
  parsed_line = JSON.parse(line.gsub('\u0000', ''))
  next if existing_conversations.include?(parsed_line['id'])

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

  if parsed_line.dig('entities', 'urls')
    parsed_line.dig('entities', 'urls').each do |url|
      next if url['expanded_url'].size > 2048
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

  if parsed_line.key?('context_annotations')
    parsed_line['context_annotations'].each do |c_annotation|
      unless existing_domains.include?(c_annotation['domain']['id'])
        context_domain << {id: c_annotation['domain']['id'],
                           name: c_annotation['domain']['name'],
                           description: c_annotation['domain']['description']}

        existing_domains << c_annotation['domain']['id']
      end

      unless existing_entities.include?(c_annotation['entity']['id'])
        context_entity << {id: c_annotation['entity']['id'],
                             name: c_annotation['entity']['name'],
                             description: c_annotation['entity']['description']}

        existing_entities << c_annotation['entity']['id']
      end

      context_annotations << {conversation_id: parsed_line['id'],
                              context_domain_id: c_annotation['domain']['id'],
                              context_entity_id: c_annotation['entity']['id']}
    end
  end

  if parsed_line.dig('entities', 'hashtags')
    parsed_line.dig('entities', 'hashtags').each do |hashtag|
      if existing_hashtags.key?(hashtag['tag'])
        conversation_hashtags << {conversation_id: parsed_line['id'],
                                  hashtag_id: existing_hashtags[hashtag['tag']]}
      else
        new_hashtags << {id: last_hashtag_id,
                         tag: hashtag['tag']}
        existing_hashtags["#{hashtag['tag']}"] = last_hashtag_id

        conversation_hashtags << {conversation_id: parsed_line['id'],
                                  hashtag_id: last_hashtag_id}

        last_hashtag_id += 1
      end
    end
  end

  existing_conversations << parsed_line['id']

  rows += 1

  if rows % batch_size == 0
    DATABASE[:authors].multi_insert(array_of_null_authors)        # TODO odstran upsert!!!
    DATABASE[:conversations].multi_insert(array_of_conversations)
    DATABASE[:links].multi_insert(links)
    DATABASE[:hashtags].multi_insert(new_hashtags)
    DATABASE[:conversation_hashtags].multi_insert(conversation_hashtags)
    DATABASE[:annotations].multi_insert(annotations)
    DATABASE[:context_domains].multi_insert(context_domain)
    DATABASE[:context_entities].multi_insert(context_entity)
    DATABASE[:context_annotations].multi_insert(context_annotations)

    puts "#{batch_number} - #{array_of_conversations.size}"
    array_of_conversations.clear
    array_of_null_authors.clear
    links.clear
    annotations.clear
    new_hashtags.clear
    conversation_hashtags.clear
    context_domain.clear
    context_entity.clear
    context_annotations.clear

    batch_number += 1

    csv_array << [Time.now.strftime('%Y-%m-%dT%H:%M%z'), seconds_to_hms(Time.now - start_time), seconds_to_hms(Time.now - batch_time)]
    batch_time = Time.now
  end
end

DATABASE[:authors].multi_insert(array_of_null_authors)
DATABASE[:conversations].multi_insert(array_of_conversations)
DATABASE[:links].multi_insert(links)
DATABASE[:hashtags].multi_insert(new_hashtags)
DATABASE[:conversation_hashtags].multi_insert(conversation_hashtags)
DATABASE[:annotations].multi_insert(annotations)
DATABASE[:context_domains].multi_insert(context_domain)
DATABASE[:context_entities].multi_insert(context_entity)
DATABASE[:context_annotations].multi_insert(context_annotations)

puts "#{batch_number} - #{array_of_conversations.size}"
csv_array << [Time.now.strftime('%Y-%m-%dT%H:%M%z'), seconds_to_hms(Time.now - start_time), seconds_to_hms(Time.now - batch_time)]
array_of_conversations.clear
array_of_null_authors.clear
links.clear
annotations.clear
new_hashtags.clear
conversation_hashtags.clear
context_domain.clear
context_entity.clear
context_annotations.clear

=end

references_import(csv_array, start_time)  # 9.7 RAM - one run

CSV.open("pdt_adam-zak-references.csv", "w") do |csv|
  csv_array.each do |time|
    csv << time
  end
end

puts "Import has ended :)"