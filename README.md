# Спринт 8

Привет, ревьювер!

Меня зовут Карина, я работаю в сфере Data Engineering. Буду рада комментариям, ко мне можно на «ты»

---

## 1. 

```python
restaurant_read_stream_df = spark.readStream \
    .format("kafka") \
    .option("subscribe", "student.topic.cohort32.yc-user_in") \
    ...
    .load()
```

Подключаемся к Kafka и считываем сообщения с кампаниями ресторанов.

---

## 2.

```python
.withColumn("current_ts", unix_timestamp(current_timestamp()).cast("long"))
```

Извлекаем поля из JSON и оставляем только те кампании, которые действуют «прямо сейчас».

---

## 3. 

```python
.option('dbtable', 'subscribers_restaurants')
```

Загружаем таблицу с подписчиками ресторанов, чтобы знать, кому отправлять кампанию.

---

## 4.
Джойнимся 
```python
joined_df = filtered_read_stream_df.join(
    subscribers_restaurant_df,
    on="restaurant_id",
    how="inner"
)
```

Находим, какие клиенты подписаны на те рестораны, у которых сейчас активна акция.


## 5

```python
.withColumn("trigger_datetime_created", unix_timestamp(current_timestamp()).cast("long"))
```

Поле `trigger_datetime_created` показывает момент, когда система "отправила" уведомление.


## 6.
Отправляем данные в постгрю

```python
.option("dbtable", "public.subscribers_feedback")
```

Записываем информацию о сработавших кампаниях в таблицу `subscribers_feedback`


```python
.option("topic", "student.topic.cohort32.yc-user_out")
```

Отправляем уведомления в Kafka для дальнейшей обработки

Спасибо за проверку! Буду рада фидбеку
