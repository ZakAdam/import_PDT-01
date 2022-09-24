require 'sequel'
require 'dotenv'
load 'create_tables.rb'

Dotenv.load

DATABASE = Sequel.connect(adapter: :postgres,
                    user: ENV.fetch("POSTGRES_USER"),
                    password: ENV.fetch("POSTGRES_PASSWORD"),
                    host: ENV.fetch("POSTGRES_HOST"),
                    port: ENV.fetch("POSTGRES_PORT"),
                    database: ENV.fetch("POSTGRES_DB"),
                    max_connections: 10)
                    #logger: Logger.new('log/db.log'))

puts "lol"
