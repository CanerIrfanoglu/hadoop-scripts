ratings = LOAD '/user/maria_dev/ml-100k/u.data' AS (userID:int, movieID:int, rating:int, ratingTime:int);

metadata = LOAD '/user/maria_dev/ml-100k/u.item' USING PigStorage('|')
	AS (movieID:int, movieTitle:chararray, releaseDate:chararray, videoRelease:chararray, imdbLink:chararray);
    
nameLookup = FOREACH metadata GENERATE movieID, movieTitle,
	ToUnixTime(ToDate(releaseDate, 'dd-MMM-yyyy')) AS releaseTime;
    
ratingsByMovie = GROUP ratings BY movieID;

countRatings = FOREACH ratingsByMovie GENERATE group AS movieID, AVG(ratings.rating) AS avgRating, COUNT(ratings.rating) AS countRating;

NoStarMovies = FILTER countRatings BY avgRating < 2.0;

NoStarsWithData = JOIN NoStarMovies BY movieID, nameLookup BY movieID;

worstNoStars = ORDER NoStarsWithData BY NoStarMovies::countRating DESC;

DUMP worstNoStars;
