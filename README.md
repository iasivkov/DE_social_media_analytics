# DE_social_media_analytics
Основная цель — создать удобную систему для хранения и аналитики неструктурированной информации, поступающей из бэкэнда социальной сети.

## Задачи Data Lake:
  - хранить историю событий;
  - инкрементально добавлять историю постов и сообщений каждый день;
  - предоставлять витрины для аналитики;
  - готовить данные для обучения моделей.
    
## Используемые источники и форматы входных данных.
 **Инкрементальные данные**
  JSON-документы, которые приходят из бэкенда:
  - с информацией о сообщениях: личным, в групповой чат, в канал, в ленту;
  - с информацией о подписках на группы;
  - с информацией о реакциях пользователей.

  JSON-документы объединены в одну таблицу событий event с партитицией по типу события (message, subscription, reaction). 
  - с информацией о сообщениях: личным, в групповой чат, в канал, в ленту;
  - с информацией о подписках на группы;
  - с информацией о реакциях пользователей.

  JSON-документы объединены в одну таблицу событий event с партитицией по типу события (message, subscription, reaction). 
  
  **Справочники**
  Приходят из БД PostgreSQL:
  - `channels` и `channel_admins` — список каналов с их админами;
  - `tags_verified` — список общедоступных тегов;
  - `users` — таблица с пользователями;
  - `group` — таблица с группам.

## Слои хранения HDFS
  - **STAGE** — таблица `events` с сырыми данными из бэкенда в формате ***json*** партиционированная по типу события; 10 справочников в формате parquet (на каждый тип справочника по два файла — один с сегодняшней информацией, один с историческими данными, партиционированными по дням)
  - **ODS** — таблица `events` партиционированная по дням и типу события, хранится в формате ***parquet***
  - **Data Mart** — витрины для аналитики, `user_interests` — предпочтения конкретного пользователя, `connection_interests` — предпочтения тех пользователей, с которыми общался данный пользователь.

## Структура хранилища
**STAGE** 

`/user/master/events` — таблица events с сырыми данными

`/user/master/snapshots` — директория со справочниками

**ODS**

`/user/iasivkov/data/events` — таблица events партиционированная по дням и типу события

**Data Mart** 

`/user/iasivkov/data/analytics` — директория с витринами

## Описание витрин

### Витрина с тэгами-кандидатами
`tag` — строка с тегом;
`suggested_count` — количество уникальных пользователей, использовавших тэг. 

### Витрина интересов пользователей

`user_id` — id пользователя;
`tag_top_1` — top-1 тег в постах пользователя за заданный период;
`tag_top_2` — top-2 тег в постах пользователя;
`tag_top_3` — top-3 тег в постах пользователя;
`like_tag_top_1` — top-1 тег в лайках пользователя за заданный период;
`like_tag_top_2` — top-2 тег в лайках;
`like_tag_top_3` — top-3 тег в лайках;
`dislike_tag_top_1` — top-1 тег в дизлайках пользователя за заданный период;
`dislike_tag_top_2` — top-2 тег в дизлайках;
`dislike_tag_top_3` — top-3 тег в дизлайках.

### Витрина интересов среди контактов пользователей

`user_id` — id пользователя;
`direct_like_tag_top_1` — самый частый тег в like_tag_top_1 за 7 дней среди тех, с кем пользователь общался в личных сообщениях; 
`direct_like_tag_top_2` — самый частый тег в like_tag_top_2 за 7 дней среди тех, с кем пользователь общался в личных сообщениях;
`direct_like_tag_top_3` — самый частый тег в like_tag_top_3 за 7 дней среди тех, с кем пользователь общался в личных сообщениях;
`direct_dislike_tag_top_1` — самый частый тег в dislike_tag_top_1 за 7 дней среди тех, с кем пользователь общался в личных сообщениях; 
`direct_dislike_tag_top_2` — самый частый тег в dislike_tag_top_2 за 7 дней среди тех, с кем пользователь общался в личных сообщениях;
`direct_dislike_tag_top_3` — самый частый тег в dislike_tag_top_3 за 7 дней среди тех, с кем пользователь общался в личных сообщениях;
`sub_verified_tag_top_1` — топ 1 верифицированных на момент расчёта тегов в сообщениях каналов, на которые подписан пользователь, опубликованных за 7 дней;
`sub_verified_tag_top_2` — топ 2 верифицированных на момент расчёта тегов в сообщениях каналов, на которые подписан пользователь, опубликованных за 7 дней;
`sub_verified_tag_top_3` — топ 3 верифицированных на момент расчёта тегов в сообщениях каналов, на которые подписан пользователь, опубликованных за 7 дней.

## Пользователи хранилища и ролевые модели:
  - Дата инжинеры имеют доступ на чтение слоя **STAGE** и чтение/запись слоёв **Data Mart**  и **ODS**
  - Аналитики и специалисты по Data Science имеют доступ на чтение **Data Mart**  и **ODS** слоёв.
