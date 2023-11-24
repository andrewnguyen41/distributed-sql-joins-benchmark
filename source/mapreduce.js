const mysql = require('mysql2/promise');
const { fetchMovieByGenre, fetchRatingsForMovies } = require('./db-utils');

// Perform the MapReduce-style join for movie ID 123
async function performMapReduceJoin() {
    try {
        const genre = "Drama";
        const start = Date.now();
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

        const end = Date.now();
        // console.log(`first result record: ${JSON.stringify(joinResult[0])}`)
        console.log(`Join completed, total time: ${end - start}ms`)

    } catch (error) {
        console.error('Error performing MapReduce join:', error);
    }
}

// Execute the MapReduce join function
performMapReduceJoin();
