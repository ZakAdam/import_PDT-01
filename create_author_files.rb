require 'zlib'
require 'json'

# JSON.parse(string) parsne tie /u432 znaky :)
#File.open(filepath) do |file|

# authors ma 55001x100 = 5 500 100  | 5  mil. users :)

def create_smaller_author_files
  filepath = '/home/adam/Downloads/authors.jsonl.gz'
  batch_size = 100000
  i = 0

  Zlib::GzipReader.open(filepath) do |file|
    file.lazy.each_slice(batch_size) do |lines|
      # do something with batch of lines

      File.open("/home/adam/Downloads/autori_#{i}.jsonl", 'w') do |f|
        lines.each do |line|
          f.write(line)
        end
      end
      i += 1
      #if i == 1
      #  puts 'Endujem sa :)'
      #  return
      #end
    end
  end
end

=begin
File.open("/home/adam/Downloads/autori_0.jsonl", 'r') do |file|
  file.each_line do |line|
    puts line.class
    lol = JSON.parse(line)
    puts lol.class
    puts lol["name"]

    i += 1
    if i == 5
      puts 'Endujem sa :)'
      exit
    end
  end
end
=end
