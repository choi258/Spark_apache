Python based

/* Query 2 */ 
SELECT DISTINCT m.title 
FROM movies m, ratings r 
WHERE 
	m.movieid = r.movieid 
	AND r.rating = 5;

/* Query 3 */ 
SELECT u.occupation, count(u.occupation) AS cnt 
FROM users AS u 
GROUP BY occupation;

/* Query 4 */ 
SELECT m.title, ROUND(AVG(r.rating), 1) 
FROM movies m, ratings r 
WHERE 
	m.movieid = r.movieid 
	AND m.title LIKE "%(1998)%" 
GROUP BY m.title;

/* Query 5 */ 
SELECT u.age, ROUND(AVG(r.rating), 1) 
FROM ratings r, users u 
WHERE 
	r.userid = u.userid 
GROUP BY u.age;