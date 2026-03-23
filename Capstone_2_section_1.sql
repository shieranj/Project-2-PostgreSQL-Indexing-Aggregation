--a. Count how many transactions are in the orders table.
SELECT COUNT(orders.id) order_count
FROM public.orders AS orders;

--b. Calculate the total order value from all transactions in the orders table.
SELECT SUM(orders.total) AS total_sales
FROM public.orders AS orders;

--c. Find the 10 products that most often give discounts in the orders table, based on the product title from the products table.
SELECT orders.product_id, products.title, COUNT(orders.discount) AS discount_count
FROM public.products AS products
JOIN public.orders AS orders
	ON products.id = orders.product_id
GROUP BY orders.product_id, products.title
ORDER BY COUNT(orders.discount) DESC
LIMIT 10;

--d. Calculate the total number of orders in the orders table, grouped by product category from the products table.
WITH category_sales AS (
	SELECT products.category , SUM(orders.total ) AS total_sales, COUNT(orders.id) AS order_count
	FROM public.products AS products
	JOIN public.orders AS orders
		ON products.id = orders.product_id
	GROUP BY products.category
)
SELECT *
FROM category_sales 
ORDER BY category_sales.total_sales DESC;

--e. Calculate the total number of orders in the orders table for each product title in the products table that has a rating ≥ 4.
WITH products_ratings AS (
	SELECT products.id, products.title , COUNT(orders.id) AS order_count, products.rating 
	FROM public.products AS products
	JOIN public.orders AS orders
		ON products.id = orders.product_id
	GROUP BY products.id, products.title
)
SELECT *
FROM products_ratings 
WHERE rating >= 4
ORDER BY rating DESC;

--f. Get a list of reviews for products in the category ‘Doohickey’, where the review rating ≤ 3.
WITH review_list AS (
	SELECT reviews.created_at , reviews.body, reviews.rating, products.category AS product_category
	FROM public.reviews AS reviews
	JOIN public.orders AS orders
		ON reviews.product_id = orders.product_id 
	JOIN public.products AS products
		ON products.id = orders.product_id
)
SELECT *
FROM review_list 
WHERE product_category='Doohickey' AND rating <= 3
ORDER BY created_at DESC;

--g. Find how many unique sources exist in the users table.
SELECT COUNT(DISTINCT users."source") AS distinct_source
FROM public.users AS users;

--h. Count the total number of users in the users table whose email domain is gmail.com.
SELECT COUNT(users.id) AS total_user_gmail
FROM public.users AS users 
WHERE users.email LIKE '%gmail.com%';

--i. Get a list of id, title, price, and created_at from the products table where the price is between 30 and 50.
SELECT products.id, products.title, products.price, products.created_at
FROM public.products AS products 
WHERE products.price BETWEEN 30 AND 50
ORDER BY products.created_at ASC;

--j.Get a list of name, email, address, and birth_date from the users table for users born after 1997. Create this in the format of a database view.
DROP VIEW IF EXISTS users_born_after_1997;

CREATE OR REPLACE VIEW users_born_after_1997 AS
SELECT users.name,
	users.email, 
	users.address, 
	CAST(users.birth_date AS date) AS birth_Date
FROM public.users AS users
WHERE EXTRACT(YEAR FROM CAST(users.birth_date AS date)) >1997;

--k. Get a list of id, created_at, title, category, and vendor from the products table where the title appears more than once (duplicate titles).
WITH product_duplicate_count AS (
	SELECT 
		products.id, 
		products.created_at, 
		products.title, 
		products.category,
		products.vendor,
		ROW_NUMBER() OVER(PARTITION BY products.title) AS row_num,
		COUNT(*) OVER (PARTITION BY products.title) AS title_count
	FROM public.products AS products
)
SELECT *
FROM product_duplicate_count 
WHERE title_count > 1;

SELECT exists (SELECT * FROM information_schema.tables 
WHERE table_name = 'orders')

SELECT current_database()
