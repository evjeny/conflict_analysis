import sqlite3

from dostoevsky.tokenization import RegexTokenizer
from dostoevsky.models import FastTextSocialNetworkModel


def create_message_replies_table(connection: sqlite3.Connection):
    connection.executescript(
        """
        DROP TABLE IF EXISTS message_replies;
        CREATE TABLE message_replies(
            message_id INTEGER,
            replies_count INTEGER
        );
        """
    )
    connection.commit()


def extract_message_replies(connection: sqlite3.Connection):
    connection.execute(
        """
        WITH unique_ids AS (
            SELECT DISTINCT message_id FROM answer
        )

        INSERT INTO message_replies(message_id, replies_count)
        SELECT reply_to_msg_id as message_id, COUNT(*) as replies_count FROM answer
        WHERE reply_to_msg_id IN unique_ids AND text != ""
        GROUP BY reply_to_msg_id
        RIGHT JOIN unique_ids ON unique_ids.message_id = reply_to_msg_id ; -- add 0 to replies_count if there are not items
        """
    )


def create_sentiment_table(connection: sqlite3.Connection):
    connection.executescript(
        """
        DROP TABLE IF EXISTS sentiment;
        CREATE TABLE sentiment(
            message_id INTEGER,
            value REAL
        );
        """
    )
    connection.commit()


def extract_sentiments(connection: sqlite3.Connection):
    tokenizer = RegexTokenizer()
    model = FastTextSocialNetworkModel(tokenizer=tokenizer)

    def _text_to_sentiment(text: str) -> float:
        s = model.predict([text])[0]
        return s.get("positive", 0) - s.get("negative", 0)
    
    connection.create_function("SENTIMENT", 1, _text_to_sentiment)
    connection.execute(
        """
        INSERT INTO sentiment(message_id, value)
        SELECT message_id, SENTIMENT(text) FROM answer
        """
    )


def main(db_name: str):
    with sqlite3.connect(db_name) as connection:
        create_message_replies_table(connection)
        extract_message_replies(connection)

        create_sentiment_table(connection)
        extract_sentiments(connection)


if __name__ == "__main__":
    main("result.db")