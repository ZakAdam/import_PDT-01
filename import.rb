require 'sequel'    # library that enables ORM for database
require 'dotenv'    # library that loads environmental variables
require 'set'       # set data type -> allows O(1) search in array - explained in documentation
require 'zlib'      # library that works with .gz files - for reading files
require 'json'      # library that parses string to Hash (better JSON in RUby) object
require 'csv'       # library for CSV file export
load 'create_tables.rb'         # load file, that checks tables
load 'authors_import.rb'        # file, that imports authors
load 'references_import.rb'     # file, that imports conversation references
load 'conversation_import.rb'   # file that imports conversations

Dotenv.load       # Load all env variables

DATABASE = Sequel.connect(adapter: :postgres,                 # options for database connections
                    user: ENV.fetch("POSTGRES_USER"),
                    password: ENV.fetch("POSTGRES_PASSWORD"),
                    host: ENV.fetch("POSTGRES_HOST"),
                    port: ENV.fetch("POSTGRES_PORT"),
                    database: ENV.fetch("POSTGRES_DB"),
                    max_connections: 10)

# method, that converts seconds to minutes and seconds, copied and edited from given link
def seconds_to_hms(sec)
  # https://stackoverflow.com/questions/28908214/converting-seconds-into-hours-only-using-ruby-in-built-function-except-the-day
  "%02d:%02d" % [sec / 60, sec % 60]
end

puts "Start je: #{Time.now}"

start_time = Time.now     # start time of program
csv_array = []            # array of times, written to CSV file at the end of execution

create_tables(DATABASE)                       # method, that creates if tables exists, if no, creates them
authors_import(csv_array ,start_time)         # method, that inserts authors
conversations_import(csv_array, start_time)   # method, that inserts conversations
references_import(csv_array, start_time)      # method, that inserts references

# block which writes, all lines from csv_array to csv file
CSV.open("pdt_adam-zak_v2.csv", "w") do |csv|
  csv_array.each do |time|
    csv << time
  end
end

puts "Import has ended :)"