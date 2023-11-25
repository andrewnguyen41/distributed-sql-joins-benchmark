const mysql = require('mysql2/promise');
const { fetchMovieByGenre, fetchRatingsForMovies } = require('./db-utils');

// Perform the Broadcast join for movie ID 123
async function performBroadcastJoin() {
    try {
        const genre = "Drama";
        const start = Date.now();

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

        // Convert ratings to a dictionary for efficient lookup
        const ratingsDict = {};
        ratings.forEach(rating => {
            const movieId = rating.movieId;
            if (!ratingsDict[movieId]) {
                ratingsDict[movieId] = [];
            }
            ratingsDict[movieId].push({
                userId: rating.userId,
                rating: rating.rating,
                timestamp: rating.timestamp,
            });
        });

        // Perform the Broadcast Join
        const joinResult = movies.map(movie => {
            const movieId = movie.movieId;
            const movieRatings = ratingsDict[movieId] || [];

            // Construct the joined data
            return {
                movieId: movieId,
                title: movie.title,
                genres: movie.genres,
                ratings: movieRatings,
            };
        });

        const end = Date.now();
        // console.log(`first result record: ${JSON.stringify(joinResult[0])}`);
        console.log(`Join completed, total time: ${end - start}ms`);
    } catch (error) {
        console.error('Error performing Broadcast join:', error);
    }
}

// Execute the Broadcast join function
performBroadcastJoin();
