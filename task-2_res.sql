Create Table test.winemag(
    nrow UInt32,
    country String,
    description String,
    designation String,
    points Uint8,
    price float,
    province String,
    region_1 String,
    region_2 String,
    taster_name String,
    taster_twitter_handle String,
    title String,
    variety String,
    winery String
    )
    Engine = MergeTree()
	order by nrow;

clickhouse-client -h 127.0.0.1 -u default --password qw123456 --format_csv_delimiter="," --query "insert into test.winemag FORMAT CSV" < winemag-data-130k-v2.csv

# оставить только непустые значения для названий стран и цен
Select COUNT(*)
From winemag
Where notEmpty(country) and price > 0

# найти максимальную цену для каждой страны
Select country, MAX(price) as mprice
From winemag
Where notEmpty(country) and price > 0
GROUP BY country

# вывести топ-10 стран с самыми дорогими винами (country, max_price)
Select country, quantile(0.5)(price) as max_price
From winemag
Where notEmpty(country) and price > 0
GROUP BY country
ORDER by max_price DESC
LIMIT 10

# определить как высокая цена коррелирует с оценкой дегустатора (насколько дорогие вина хорошие)
# учесть в выборке также регион производства
Select taster_name, country, region_1, price, points
From winemag
Where notEmpty(taster_name)
  and notEmpty(country)
  and notEmpty(region_1)
  and points > 0
  and price > 0
Order By taster_name, country, region_1, price DESC
LIMIT 100

# Вывод:
# цена вина и оценка дегустатора живут по своим законам.
# переплачивать за дорогие вина не имеет смысла.