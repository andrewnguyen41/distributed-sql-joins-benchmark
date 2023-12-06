const mysql = require('mysql2/promise');
const { fetchMovieByGenre, fetchRatingsForMovies } = require('./db-utils');

async function performBucketedJoin() {
  try {
    const genres = ["Drama", "Comedy", "Documentary"]
    const start = Date.now();
    for (let genre of genres) {
      const numBuckets = 10; // Adjust the number of buckets as needed

      // Fetch movies and ratings
      const movies = await fetchMovieByGenre(genre);
      console.log(`movies count ${movies.length}`);
      if (movies.length <= 0) {
        console.log('Movies not found');
        return;
      }

      const movieIds = movies.map(movie => movie.movieId);
      const ratings = await fetchRatingsForMovies(movieIds);
      console.log(`ratings count ${ratings.length}`);

      // Initialize buckets
      const movieBuckets = Array.from({ length: numBuckets }, () => []);
      const ratingBuckets = Array.from({ length: numBuckets }, () => []);

      // Assign movies and ratings to buckets based on the join key
      movies.forEach(movie => {
        const bucketIndex = movie.movieId % numBuckets;
        movieBuckets[bucketIndex].push(movie);
      });

      ratings.forEach(rating => {
        const bucketIndex = rating.movieId % numBuckets;
        ratingBuckets[bucketIndex].push(rating);
      });

      // Perform the Bucketed Join
      const joinResult = [];
      for (let bucketIndex = 0; bucketIndex < numBuckets; bucketIndex++) {
        const bucketMovies = movieBuckets[bucketIndex];
        const bucketRatings = ratingBuckets[bucketIndex];

        for (let movie of bucketMovies) {
          const movieRatings = bucketRatings.filter(rating => rating.movieId === movie.movieId);

          // Construct the joined data
          joinResult.push({
            movieId: movie.movieId,
            title: movie.title,
            genres: movie.genres,
            ratings: movieRatings.map(rating => ({
              userId: rating.userId,
              rating: rating.rating,
              timestamp: rating.timestamp,
            })),
          });
        }
      }
    }

    const end = Date.now();
    const memoryUsage = process.memoryUsage();
    const totalTime = end - start;
    console.log(`Join completed, total time: ${totalTime}ms`)
    console.log('Total memory usage:', memoryUsage);
    console.log(`Average time: ${totalTime / genres.length}ms`)
    console.log('Average memory usage:', memoryUsage.rss / genres.length);
  } catch (error) {
    console.error('Error performing Bucketed join:', error);
  }
}

// Execute the Bucketed join function
performBucketedJoin();
