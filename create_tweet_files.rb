require 'zlib'

filepath = '/home/adam/Downloads/conversations.jsonl.gz'
batch_size = 100
i = 0

# 10000x3106 = 31 060 000 tweets :)

Zlib::GzipReader.open(filepath) do |file|
  file.lazy.each_slice(batch_size) do |lines|
    #puts line.class
    #lol = JSON.parse(line)
    #puts lol.class
    #puts lol["name"]

    File.open("/home/adam/Downloads/conversations_#{i}.jsonl", 'w') do |f|
      lines.each do |line|
        f.write(line)
      end
    end

    puts i
    i += 1
    exit
    #if i == 5
    #  puts 'Endujem sa :)'
    #  exit
    #end
  end
end
