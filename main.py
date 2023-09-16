import redis

# Подключение к Redis
r = redis.StrictRedis(host='localhost', port=6379, db=0)
print(r)
r.flushdb()  # очистить пред. базу


# Функция для добавления записи книги
def add_book(book_data):
    book_id = book_data["ID_book"]
    r.hmset(f"Books:{book_id}", book_data)

# Функция для добавления записи автора


def add_author(author_data):
    author_id = author_data["Author_ID"]
    r.hmset(f"Authors:{author_id}", author_data)

# Функция для добавления записи жанра


def add_genre(genre_data):
    genre_id = genre_data["Genre_ID"]
    r.hmset(f"Genres:{genre_id}", genre_data)

# Функция для добавления записи города и издательства


def add_city_and_publishing_house(city_data):
    city_id = city_data["City_ID"]
    r.hmset(f"CitiesAndPublishingHouses:{city_id}", city_data)


# Добавляем данные в таблицу Authors
author1 = {
    "Author_ID": 1,
    "Author_Name": "Автор 1"
}
add_author(author1)

# Добавляем данные в таблицу Genres
genre1 = {
    "Genre_ID": 1,
    "Genre_Name": "Жанр 1"
}
add_genre(genre1)

# Добавляем данные в таблицу CitiesAndPublishingHouses
city1 = {
    "City_ID": 1,
    "City_Name": "Город 1",
    "Publishing_House_Name": "Издательство 1"
}
add_city_and_publishing_house(city1)

# Добавляем данные в таблицу Books
book1 = {
    "ID_book": 1,
    "Author_ID": 1,
    "Title": "Книга 1",
    "City": "Город 1",
    "Publisher": "Издательство 1",
    "Year": 2023,
    "Cover_Type": "Тип обложки 1",
    "Price": 1000.0,
    "Discount": 0.2
}
add_book(book1)

# Получаем данные о книге по её ID
book_id_to_retrieve = 1
book_data = r.hgetall(f"Books:{book_id_to_retrieve}")
print("Данные о книге:")
for key, value in book_data.items():
    print(f"{key.decode('utf-8')}: {value.decode('utf-8')}")

import redis

# Подключение к Redis
r = redis.StrictRedis(host='localhost', port=6379, db=0)

# 1) List of books
books = r.keys("Books:*")
for book_key in books:
    book_data = r.hgetall(book_key)
    print("Book ID:", book_data[b'ID_book'].decode('utf-8'))
    print("Author:", book_data[b'Author_ID'].decode('utf-8'))
    print("Title:", book_data[b'Title'].decode('utf-8'))
    print("City:", book_data[b'City'].decode('utf-8'))
    print("Publisher:", book_data[b'Publisher'].decode('utf-8'))
    print("Year:", book_data[b'Year'].decode('utf-8'))
    print("Cover Type:", book_data[b'Cover_Type'].decode('utf-8'))
    print("Price:", book_data[b'Price'].decode('utf-8'))
    print("Discount:", book_data[b'Discount'].decode('utf-8'))
    print()

# 2) List of authors (no repetitions)
authors = r.keys("Authors:*")
unique_authors = set()
for author_key in authors:
    author_data = r.hgetall(author_key)
    unique_authors.add(author_data[b'Author_Name'].decode('utf-8'))
print("List of authors (no repetitions):", unique_authors)

# 3) List of genres (no repetitions)
genres = r.keys("Genres:*")
unique_genres = set()
for genre_key in genres:
    genre_data = r.hgetall(genre_key)
    unique_genres.add(genre_data[b'Genre_Name'].decode('utf-8'))
print("List of genres (no repetitions):", unique_genres)

# 4) List of cities and publishing houses (no repetitions)
cities_publishing_houses = r.keys("CitiesAndPublishingHouses:*")
unique_cities_publishing_houses = set()
for city_key in cities_publishing_houses:
    city_data = r.hgetall(city_key)
    unique_cities_publishing_houses.add(city_data[b'City_Name'].decode('utf-8'))
    unique_cities_publishing_houses.add(city_data[b'Publishing_House_Name'].decode('utf-8'))
print("List of cities and publishing houses (no repetitions):", unique_cities_publishing_houses)

# 5) List of books by the specified author
specified_author = "Автор 1"
specified_author_books = []
for book_key in books:
    book_data = r.hgetall(book_key)
    if book_data[b'Author_ID'].decode('utf-8') == specified_author:
        specified_author_books.append({
            "Author": book_data[b'Author_ID'].decode('utf-8'),
            "Title": book_data[b'Title'].decode('utf-8'),
            "City": book_data[b'City'].decode('utf-8'),
            "Publisher": book_data[b'Publisher'].decode('utf-8'),
            "Year": book_data[b'Year'].decode('utf-8'),
            "Cover Type": book_data[b'Cover_Type'].decode('utf-8'),
            "Price": book_data[b'Price'].decode('utf-8')
        })
print("List of books by the specified author:", specified_author_books)

# 6) List of books from the specified publishing house
specified_publishing_house = "Издательство 1"
specified_publishing_house_books = []
for book_key in books:
    book_data = r.hgetall(book_key)
    if book_data[b'Publisher'].decode('utf-8') == specified_publishing_house:
        specified_publishing_house_books.append({
            "Author": book_data[b'Author_ID'].decode('utf-8'),
            "Title": book_data[b'Title'].decode('utf-8'),
            "City": book_data[b'City'].decode('utf-8'),
            "Publisher": book_data[b'Publisher'].decode('utf-8'),
            "Year": book_data[b'Year'].decode('utf-8'),
            "Cover Type": book_data[b'Cover_Type'].decode('utf-8'),
            "Price": book_data[b'Price'].decode('utf-8')
        })
print("List of books from the specified publishing house:", specified_publishing_house_books)

# 7) List of books of the specified genre
specified_genre = "Жанр 1"
specified_genre_books = []
for book_key in books:
    book_data = r.hgetall(book_key)
    book_genre_id = book_data[b'Genre_ID'].decode('utf-8')
    genre_data = r.hgetall(f"Genres:{book_genre_id}")
    if genre_data[b'Genre_Name'].decode('utf-8') == specified_genre:
        specified_genre_books.append({
            "Author": book_data[b'Author_ID'].decode('utf-8'),
            "Title": book_data[b'Title'].decode('utf-8'),
            "City": book_data[b'City'].decode('utf-8'),
            "Publisher": book_data[b'Publisher'].decode('utf-8'),
            "Year": book_data[b'Year'].decode('utf-8'),
            "Cover Type": book_data[b'Cover_Type'].decode('utf-8'),
            "Price": book_data[b'Price'].decode('utf-8')
        })
print("List of books of the specified genre:", specified_genre_books)

# 8) List of books published in the specified range of years
start_year = 2020
end_year = 2025
specified_year_range_books = []
for book_key in books:
    book_data = r.hgetall(book_key)
    book_year = int(book_data[b'Year'].decode('utf-8'))
    if start_year <= book_year <= end_year:
        specified_year_range_books.append({
            "Author": book_data[b'Author_ID'].decode('utf-8'),
            "Title": book_data[b'Title'].decode('utf-8'),
            "City": book_data[b'City'].decode('utf-8'),
            "Publisher": book_data[b'Publisher'].decode('utf-8'),
            "Year": book_data[b'Year'].decode('utf-8'),
            "Cover Type": book_data[b'Cover_Type'].decode('utf-8'),
            "Price": book_data[b'Price'].decode('utf-8')
        })
print("List of books published in the specified range of years:", specified_year_range_books)

# 9) List of books with a price in the specified range
min_price = 500.0
max_price = 1500.0
specified_price_range_books = []
for book_key in books:
    book_data = r.hgetall(book_key)
    book_price = float(book_data[b'Price'].decode('utf-8'))
    if min_price <= book_price <= max_price:
                specified_price_range_books.append({
            "Author": book_data[b'Author_ID'].decode('utf-8'),
            "Title": book_data[b'Title'].decode('utf-8'),
            "City": book_data[b'City'].decode('utf-8'),
            "Publisher": book_data[b'Publisher'].decode('utf-8'),
            "Year": book_data[b'Year'].decode('utf-8'),
            "Cover Type": book_data[b'Cover_Type'].decode('utf-8'),
            "Price": book_data[b'Price'].decode('utf-8')
        })
print("List of books with a price in the specified range:", specified_price_range_books)

# 10) The total number and total cost of books by the specified author
author_total_cost = 0
author_total_books = 0
for book_key in books:
    book_data = r.hgetall(book_key)
    if book_data[b'Author_ID'].decode('utf-8') == specified_author:
        book_price = float(book_data[b'Price'].decode('utf-8'))
        author_total_cost += book_price
        author_total_books += 1
print(f"Total number of books by {specified_author}: {author_total_books}")
print(f"Total cost of books by {specified_author}: {author_total_cost}")

# 11) Number of books in hard and soft covers
hard_cover_count = 0
soft_cover_count = 0
for book_key in books:
    book_data = r.hgetall(book_key)
    cover_type = book_data[b'Cover_Type'].decode('utf-8')
    if cover_type == "Тип обложки 1":  # Поменяйте на соответствующий тип обложки для твердой обложки
        hard_cover_count += 1
    else:
        soft_cover_count += 1
print("Number of books in hard covers:", hard_cover_count)
print("Number of books in soft covers:", soft_cover_count)








# keys = r.keys('*')
# print(keys)
# # Выводим ключи и их значения
# for key in keys:
#     value = r.hgetall(key)
#     print(f'Ключ: {key}, Значение: {value}')


# Закрываем соединение с Redis
r.close()
