def create_tables(database)
  database.create_table? :authors do
     primary_key :id, type: :Bignum, null: false
     String :name, null: false
     String :username, null: false
     String :description, text: true, null: false
     Integer :followers_count, null: false
     Integer :following_count, null: false
     Integer :tweet_count, null: false
     Integer :listed_count, null: false
   end


  database.create_table? :conversations do
    primary_key :id, type: :Bignum
    foreign_key :author_id, :authors, type: :Bignum
    String :content, text: true
    FalseClass :possible_sensitive
    String :language, size: 3
    String :source, text: true
    Integer :retweet_count, null: false
    Integer :reply_count, null: false
    Integer :like_count, null: false
    Integer :quote_count, null: false
    DateTime :created_at
  end


  database.create_table? :hashtags do
    primary_key :id, type: :Bignum
    String :text, text: true, unique: true
  end


  database.create_table? :conversation_hashtags do
    primary_key :id, type: :Bignum
    foreign_key :hashtag_id, :hashtags, type: Bignum
    foreign_key :conversation_id, :conversations, type: Bignum
  end


  database.create_table? :conversation_references do
    primary_key :id, type: :Bignum
    foreign_key :conversation_id, :conversations, type: Bignum
    foreign_key :parent_id, :conversations, type: Bignum
    String :type, size: 20
  end


  database.create_table? :links do
    primary_key :id, type: :Bignum
    foreign_key :conversation_id, :conversations, type: Bignum
    String :url, size: 2048
    String :title, text: true, null: false
    String :description, text: true, null: false
  end


  database.create_table? :annotations do
    primary_key :id, type: :Bignum
    foreign_key :conversation_id, :conversations, type: Bignum
    String :value, text: true
    String :type, text: true
    BigDecimal :probability, size: [4, 3]
  end


  database.create_table? :context_domains do
    primary_key :id, type: :Bignum
    String :name
    String :description, text: true, null: false
  end


  database.create_table? :context_entities do
    primary_key :id, type: :Bignum
    String :name
    String :description, text: true, null: false
  end


  database.create_table? :context_annotations do
    primary_key :id, type: :Bignum
    foreign_key :conversation_id, :conversations, type: Bignum
    foreign_key :context_domain_id, type: Bignum
    foreign_key :context_entity_id, type: Bignum
  end
end
puts 'Zrobena tabulecka ;)'
