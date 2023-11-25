const mysql = require('mysql2/promise');
const { fetchMovieByGenre, fetchRatingsForMovies } = require('./db-utils');

// Perform the Sort-Merge join for movie ID 123
async function performSortMergeJoin() {
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

        // Sort movies and ratings based on the join key
        movies.sort((a, b) => a.movieId - b.movieId);
        ratings.sort((a, b) => a.movieId - b.movieId);

        // Perform the Sort-Merge Join
        const joinResult = [];
        let i = 0;
        let j = 0;

        while (i < movies.length && j < ratings.length) {
            const movie = movies[i];
            const rating = ratings[j];

            if (movie.movieId === rating.movieId) {
                // Construct the joined data
                joinResult.push({
                    movieId: movie.movieId,
                    title: movie.title,
                    genres: movie.genres,
                    ratings: [{
                        userId: rating.userId,
                        rating: rating.rating,
                        timestamp: rating.timestamp,
                    }],
                });
                j++;
            } else if (movie.movieId < rating.movieId) {
                i++;
            } else {
                j++;
            }
        }

        const end = Date.now();
        // console.log(`first result record: ${JSON.stringify(joinResult[0])}`);
        console.log(`Join completed, total time: ${end - start}ms`);
    } catch (error) {
        console.error('Error performing Sort-Merge join:', error);
    }
}

// Execute the Sort-Merge join function
performSortMergeJoin();
