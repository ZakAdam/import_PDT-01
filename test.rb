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
