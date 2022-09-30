def create_tables(database)
  database.create_table? :authors2 do
    primary_key :id, type: :Bignum, null: false
    String :name, null: false
    String :username, null: false
    String :description, text: true, null: false
    Integer :followers_count, null: false
    Integer :following_count, null: false
    Integer :tweet_count, null: false
    Integer :listed_count, null: false
  end

  database.create_table? :authors do
     primary_key :id, type: :Bignum
     String :name
     String :username
     String :description, text: true
     Integer :followers_count
     Integer :following_count
     Integer :tweet_count
     Integer :listed_count
   end


  database.create_table? :conversations do
    primary_key :id, type: :Bignum, null: false
    foreign_key :author_id, :authors, type: :Bignum, null: false
    String :content, text: true, null: false
    FalseClass :possible_sensitive, null: false
    String :language, size: 3, null: false
    String :source, text: true, null: false
    Integer :retweet_count
    Integer :reply_count
    Integer :like_count
    Integer :quote_count
    DateTime :created_at, null: false
  end


  database.create_table? :hashtags do
    primary_key :id, type: :Bignum, null: false
    String :text, text: true, unique: true, null: false
  end


  database.create_table? :conversation_hashtags do
    primary_key :id, type: :Bignum, null: false
    foreign_key :hashtag_id, :hashtags, type: :Bignum, null: false
    foreign_key :conversation_id, :conversations, type: :Bignum, null: false
  end


  database.create_table? :conversation_references do
    primary_key :id, type: :Bignum, null: false
    foreign_key :conversation_id, :conversations, type: :Bignum, null: false
    foreign_key :parent_id, :conversations, type: :Bignum, null: false
    String :type, size: 20, null: false
  end


  database.create_table? :links do
    primary_key :id, type: :Bignum, null: false
    foreign_key :conversation_id, :conversations, type: :Bignum, null: false
    String :url, size: 2048, null: false
    String :title, text: true
    String :description, text: true
  end


  database.create_table? :annotations do
    primary_key :id, type: :Bignum, null: false
    foreign_key :conversation_id, :conversations, type: :Bignum, null: false
    String :value, text: true, null: false
    String :type, text: true, null: false
    BigDecimal :probability, size: [4, 3], null: false
  end


  database.create_table? :context_domains do
    primary_key :id, type: :Bignum, null: false
    String :name, null: false
    String :description, text: true
  end


  database.create_table? :context_entities do
    primary_key :id, type: :Bignum, null: false
    String :name, null: false
    String :description, text: true
  end


  database.create_table? :context_annotations do
    primary_key :id, type: :Bignum, null: false
    foreign_key :conversation_id, :conversations, type: :Bignum, null: false
    foreign_key :context_domain_id, type: :Bignum, null: false
    foreign_key :context_entity_id, type: :Bignum, null: false
  end
end
puts 'Zrobene tabulecky ;)'
