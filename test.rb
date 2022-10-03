=begin
require 'objspace'
size = 35000000
lol = {}

(0..size).each do |number|
  lol["#{number}"] = '123456'
end

puts Time.now
puts ObjectSpace.memsize_of(lol)
puts lol['23322791']
puts Time.now
exit
=end

=begin
require 'zip'
i = 0

Zip::InputStream.open(StringIO.new(File.open('authors.jsonl.gz'))) do |io|
  puts io
  while entry = io.get_next_entry
    puts entry
    i += 1
    exit if i > 5
  end
end
=end
#=begin


#Zlib::GzipReader.zcat(File.open('authors.jsonl.gz')) do |file|

def test_authors_nil(database, filepath, batch_size)
  i = 0
  puts Time.now
  array_of_authors = []

  #lol = File.open('authors.jsonl')
  #File.open('authors.jsonl') do |file|
  #Zlib::GzipReader.open('authors.jsonl.gz') do |file|
    #puts "Moreeeeeeeeee: #{file.size}"

  #File.open('authors.jsonl.gz') do |file|
  #Zlib::GzipReader.zcat(File.open('authors.jsonl.gz')) do |file|
    #more = Zlib::GzipReader.new(file)

  batch_number = 0

  Zlib::GzipReader.zcat(File.open('authors.jsonl.gz')) do |line|
    parsed_line = JSON.parse(line.gsub('\u0000', ''))

    array_of_authors << {id: parsed_line['id'],
                         name: parsed_line['name'],
                         username: parsed_line['username'],
                         description: parsed_line['description'],
                         followers_count: parsed_line.dig('public_metrics', 'followers_count'),
                         following_count: parsed_line.dig('public_metrics', 'following_count'),
                         tweet_count: parsed_line.dig('public_metrics', 'tweet_count'),
                         listed_count: parsed_line.dig('public_metrics', 'listed_count')}
    i += 1

    if i % 100000 == 0
      database[:authors2].insert_conflict(:target=>:id).multi_insert(array_of_authors)
      array_of_authors = []
      puts "#{batch_number} - #{array_of_authors.size}"
      batch_number += 1
    end
  end
  database[:authors2].insert_conflict(:target=>:id).multi_insert(array_of_authors)
  puts Time.now
end

#=end

=begin
def test_upsert(database)
  array_of_authors = []

  (0..5).each do |number|
    array_of_authors << {id: 1,
                         name: 'more',
                         username: 'jezef.mihal',
                         description: 'parsed_line[xadasdz]',
                         followers_count: 44,
                         following_count: 88,
                         tweet_count: 44,
                         listed_count: 88}

  end

  #database[:authors].multi_insert(array_of_authors)
  database[:authors].insert_conflict.multi_insert(array_of_authors)
end
=end
