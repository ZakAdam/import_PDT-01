require 'sequel'
require 'set'
require 'zlib'
require 'json'

# method called from import.rb file
# imports data to 9 tables
# csv_array is filled with time values
# start_time, to calculate time differences in runtime
def conversations_import(csv_array, start_time)
   puts Time.now
   batch_size = 100000                  # size of imported batch
   batch_number = 0
   filepath = 'conversations.jsonl.gz'
   array_of_conversations = []          # array, which holds conversations, that will be imported to database
   array_of_null_authors = []           # array, which holds authors, that are not in table and will be inserted (all values except id are null)
   existing_conversations = Set[]       # Set, that holds IDs of already imported conversations
   links = []                           # array, which holds links, that will be imported to database
   annotations = []                     # array, which holds annotations, that will be imported to database
   context_annotations = []             # array, which holds context_annotations, that will be imported to database
   context_domain = []                  # array, which holds context_domains, that will be imported to database
   existing_domains = Set[]             # Set, that holds IDs of already imported context_domains
   context_entity = []                  # array, which holds context_entities, that will be imported to database
   existing_entities = Set[]            # Set, that holds IDs of already imported context_entities

   conversation_hashtags = []           # array, which holds conversation_hashtags, that will be imported to database
   new_hashtags = []                    # array, which holds hashtags, that will be imported to database
   existing_hashtags = {}               # Hash, that holds tag and ID of already imported hashtags -> {'pdt': 32}
   last_hashtag_id = 1                  # ID of last inserted hashtag
   rows = 0                             # number of row, which is being processed

   batch_time = Time.now                # start time of current batch processing

   author_ids = DATABASE[:authors].select(:id).map{|e| e[:id]}.to_set   # IDs of existing authors in database

   Zlib::GzipReader.zcat(File.open(filepath)) do |line|                   # Open stream of unzip data from .gz file
     parsed_line = JSON.parse(line.gsub('\u0000', ''))                    # First replace null bytes with empty string, than parse string to Hash data object
     next if existing_conversations.include?(parsed_line['id'].to_i)      # If conversation id is already present in table, skip this conversation

     unless author_ids.include?(parsed_line['author_id'].to_i)            # If author of conversation is not present in authors table
       array_of_null_authors << {id: parsed_line['author_id']}            # Create new author, with given ID, other values keep null
       author_ids << parsed_line['author_id'].to_i                        # add new ID to existing IDs of authors
     end

     array_of_conversations << {id: parsed_line['id'],                    # Insert new conversation row to array
                                author_id: parsed_line['author_id'],
                                content: parsed_line['text'],
                                possible_sensitive: parsed_line['possibly_sensitive'],
                                language: parsed_line['lang'],
                                source: parsed_line['source'],
                                retweet_count: parsed_line.dig('public_metrics', 'retweet_count'),
                                reply_count: parsed_line.dig('public_metrics', 'reply_count'),
                                like_count: parsed_line.dig('public_metrics', 'like_count'),
                                quote_count: parsed_line.dig('public_metrics', 'quote_count'),
                                created_at: parsed_line['created_at']}

     if parsed_line.dig('entities', 'urls')                               # If conversation has urls in entities
       parsed_line.dig('entities', 'urls').each do |url|                  # for each url in entities take one as url
         next if url['expanded_url'].size > 2048                          # skip this url if length is longer than 2048
         links << {conversation_id: parsed_line['id'],
                   url: url['expanded_url'],
                   title: url['title'],
                   description: url['description']}
       end
     end

     if parsed_line.dig('entities', 'annotations')                        # If entities has annotations
       parsed_line.dig('entities', 'annotations').each do |annotation|
         annotations << {conversation_id: parsed_line['id'],
                         value: annotation['normalized_text'],
                         type: annotation['type'],
                         probability: annotation['probability']}
       end
     end

     if parsed_line.key?('context_annotations')                           # If context_annotations are part of conversation row
       parsed_line['context_annotations'].each do |c_annotation|
         unless existing_domains.include?(c_annotation['domain']['id'].to_i)    # If context_domain is already existing, do not create new
           context_domain << {id: c_annotation['domain']['id'],           # Otherwise create new domain
                              name: c_annotation['domain']['name'],
                              description: c_annotation['domain']['description']}

           existing_domains << c_annotation['domain']['id'].to_i          # Add ID of new domain to Set
         end

         unless existing_entities.include?(c_annotation['entity']['id'].to_i)   # If entity is already existing
           context_entity << {id: c_annotation['entity']['id'],
                              name: c_annotation['entity']['name'],
                              description: c_annotation['entity']['description']}

           existing_entities << c_annotation['entity']['id'].to_i
         end

         context_annotations << {conversation_id: parsed_line['id'],       # Create new context_annotation
                                 context_domain_id: c_annotation['domain']['id'],
                                 context_entity_id: c_annotation['entity']['id']}
       end
     end

     if parsed_line.dig('entities', 'hashtags')                             # If entities has hashtags in it
       parsed_line.dig('entities', 'hashtags').each do |hashtag|
         if existing_hashtags.key?(hashtag['tag'])                          # If hashtag already exists, in hash
           conversation_hashtags << {conversation_id: parsed_line['id'],    # Insert into conversation_hashtag
                                     hashtag_id: existing_hashtags[hashtag['tag']]}
         else                                                               # Otherwise create new hashtag
           new_hashtags << {id: last_hashtag_id,                            # Create new hashtag
                            tag: hashtag['tag']}
           existing_hashtags["#{hashtag['tag']}"] = last_hashtag_id         # Insert its ID to existing hashtags

           conversation_hashtags << {conversation_id: parsed_line['id'],    # Insert into conversation_hashtag
                                     hashtag_id: last_hashtag_id}

           last_hashtag_id += 1                                             # Increment last_hashtag_id
         end
       end
     end

     existing_conversations << parsed_line['id'].to_i                        # Insert new ID to existing conversation IDs

     rows += 1                                                                # Increment rows

     if rows % batch_size == 0                                                # If this row is given batch size
       # Insert all arrays to DB on one INSERT INTO statement
       DATABASE[:authors].multi_insert(array_of_null_authors)
       DATABASE[:conversations].multi_insert(array_of_conversations)
       DATABASE[:links].multi_insert(links)
       DATABASE[:hashtags].multi_insert(new_hashtags)
       DATABASE[:conversation_hashtags].multi_insert(conversation_hashtags)
       DATABASE[:annotations].multi_insert(annotations)
       DATABASE[:context_domains].multi_insert(context_domain)
       DATABASE[:context_entities].multi_insert(context_entity)
       DATABASE[:context_annotations].multi_insert(context_annotations)

       # clear all arrays
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

       # increment batch_number
       batch_number += 1

       # Insert times to csv_array
       csv_array << [Time.now.strftime('%Y-%m-%dT%H:%M%z'), seconds_to_hms(Time.now - start_time), seconds_to_hms(Time.now - batch_time)]
       batch_time = Time.now                                                  # Update batch_time with new one
     end
   end

   # After last stream from file, last time import arrays
   DATABASE[:authors].multi_insert(array_of_null_authors)
   DATABASE[:conversations].multi_insert(array_of_conversations)
   DATABASE[:links].multi_insert(links)
   DATABASE[:hashtags].multi_insert(new_hashtags)
   DATABASE[:conversation_hashtags].multi_insert(conversation_hashtags)
   DATABASE[:annotations].multi_insert(annotations)
   DATABASE[:context_domains].multi_insert(context_domain)
   DATABASE[:context_entities].multi_insert(context_entity)
   DATABASE[:context_annotations].multi_insert(context_annotations)

   # Also clear arrays and RAM after insertion
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
end
