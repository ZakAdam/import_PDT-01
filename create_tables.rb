def create_tables(database)
  unless database.table_exists?(:authors)
    database.create_table :authors do
       primary_key :id, type: :Bignum
       String :name
       String :username
       String :description, text: true
       Integer :followers_count
       Integer :following_count
       Integer :tweet_count
       Integer :listed_count
     end
  end
end
puts 'Zrobena tabulecka ;)'
