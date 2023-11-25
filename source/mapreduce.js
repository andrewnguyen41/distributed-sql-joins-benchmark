const mysql = require('mysql2/promise');
const { fetchMovieByGenre, fetchRatingsForMovies } = require('./db-utils');

async function performMapReduceJoin() {
    try {
        const genres = ["Drama", "Comedy", "Documentary"]
        const start = Date.now();
        for (let genre of genres) {
            const movies = await fetchMovieByGenre(genre);
            console.log(`movies count ${movies.length}`)
            if (movies.length <= 0) {
                console.log('Movies not found');
                return;
            }

            const movieIds = movies.map(movie => movie.movieId);
            const ratings = await fetchRatingsForMovies(movieIds);
            console.log(`ratings count ${ratings.length}`)

            const joinResult = []
            for (let movie of movies) {
                movieRatings = ratings.filter(rating => rating.movieId == movie.movieId)
                // Construct the joined data
                joinResult.push({
                    movieId: movie.movieId,
                    title: movie.title,
                    genres: movie.genres,
                    ratings: movieRatings.map((rating) => ({
                        userId: rating.userId,
                        rating: rating.rating,
                        timestamp: rating.timestamp,
                    })),
                });
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
        console.error('Error performing MapReduce join:', error);
    }
}

// Execute the MapReduce join function
performMapReduceJoin();
