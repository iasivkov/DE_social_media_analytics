# DE_social_media_analytics
The main goal is to create a convenient system for storing and analyzing unstructured information coming from the social network's backend.

## Data Lake Tasks:
  - store event history;
  - incrementally add posts and messages history every day;
  - provide data marts for analytics;
  - prepare data for training models.

## Sources and formats of incoming data:
**Incremental data**
JSON documents received from the backend:
  - with information about messages: private, group chat, channel, and feed;
  - with information about group subscriptions;
  - with information about user reactions.

JSON documents are merged into one event table with partitioning by event type (message, subscription, reaction).

**Reference tables**
Received from a PostgreSQL database:
  - `channels` and `channel_admins` — list of channels with their admins;
  - `tags_verified` — list of public tags;
  - `users` — table of users;
  - `group` — table of groups.

## HDFS Storage Layers
  - **STAGE** — `events` table with raw data from the backend in ***json*** format, partitioned by event type; 10 reference tables in parquet format (two files per reference type — one with today's information, one with historical data partitioned by days).
  - **ODS** — `events` table partitioned by days and event type, stored in ***parquet*** format.
  - **Data Mart** — analytics data marts, `user_interests` — preferences of specific users, `connection_interests` — preferences of users who have interacted with the given user.

## Storage Structure
Continuing with the translation:

---

**STAGE**

`/user/master/events` — `events` table with raw data from the backend, partitioned by event type.

`/user/master/reference` — reference tables, partitioned by day:
  - `channels`, `channel_admins`, `tags_verified`, `users`, `group`.

**ODS**

`/user/master/ods/events` — daily partitioned table of events in `parquet` format.

**Data Mart**

- `/user/master/data_mart/user_interests` — `user_interests` table: user interests and preferences.

- `/user/master/data_mart/connection_interests` — `connection_interests` table: interests of users who have interacted with the given user.

## Data Mart Descriptions
 
### Data Mart Structure for User Interests

- `user_id` — user ID;
- `post_tag_top_1` — top-1 tag in user's posts over the selected period;
- `post_tag_top_2` — top-2 tag in posts;
- `post_tag_top_3` — top-3 tag in posts;
- `like_tag_top_1` — top-1 tag in user's likes over the selected period;
- `like_tag_top_2` — top-2 tag in likes;
- `like_tag_top_3` — top-3 tag in likes;
- `dislike_tag_top_1` — top-1 tag in user's dislikes over the selected period;
- `dislike_tag_top_2` — top-2 tag in dislikes;
- `dislike_tag_top_3` — top-3 tag in dislikes.

### Data Mart for User Connections' Interests

- `user_id` — user ID;
- `direct_like_tag_top_1` — most frequent top-1 tag in likes over 7 days among users the given user has interacted with in private messages;
- `direct_like_tag_top_2` — most frequent top-2 tag in likes over 7 days among users the given user has interacted with in private messages;
- `direct_like_tag_top_3` — most frequent top-3 tag in likes over 7 days among users the given user has interacted with in private messages;
- `direct_dislike_tag_top_1` — most frequent top-1 tag in dislikes over 7 days among users the given user has interacted with in private messages;
- `direct_dislike_tag_top_2` — most frequent top-2 tag in dislikes over 7 days among users the given user has interacted with in private messages;
- `direct_dislike_tag_top_3` — most frequent top-3 tag in dislikes over 7 days among users the given user has interacted with in private messages;
- `sub_verified_tag_top_1` — top-1 verified tag in messages from channels the user is subscribed to, published within 7 days;
- `sub_verified_tag_top_2` — top-2 verified tag in messages from channels the user is subscribed to, published within 7 days;
- `sub_verified_tag_top_3` — top-3 verified tag in messages from channels the user is subscribed to, published within 7 days.

## Storage Users and Role Models

  - Data engineers have read access to the **STAGE** layer and read/write access to the **Data Mart** and **ODS** layers.
  - Analysts and Data Science specialists have read access to the **Data Mart** and **ODS** layers.