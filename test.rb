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