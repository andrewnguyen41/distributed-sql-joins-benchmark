const mysql = require('mysql2/promise');
const { fetchMovieByGenre, fetchRatingsForMovies } = require('./db-utils');

// Perform the Bucketed join for movie ID 123
async function performBucketedJoin() {
    try {
        const genre = "Drama";
        const start = Date.now();
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

        const end = Date.now();
        // console.log(`first result record: ${JSON.stringify(joinResult[0])}`);
        console.log(`Join completed, total time: ${end - start}ms`);
    } catch (error) {
        console.error('Error performing Bucketed join:', error);
    }
}

// Execute the Bucketed join function
performBucketedJoin();
