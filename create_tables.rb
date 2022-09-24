puts 'MORE'

def create_authors
=begin
  DB.create_table :items do
    primary_key :id, type: :Bignum
    String :name
    String :username
    String :description, text: true
    Integer :followers_count
    Integer :following_count
    Integer :tweet_count
    Integer :listed_count
  end
=end
end