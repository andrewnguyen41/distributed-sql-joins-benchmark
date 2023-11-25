const mysql = require('mysql2/promise');
const { fetchMovieByGenre, fetchRatingsForMovies } = require('./db-utils');

async function performSortMergeJoin() {
    try {
        const genres = ["Drama", "Comedy", "Documentary"]
        const start = Date.now();
        for (let genre of genres) {
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
        }

        const end = Date.now();
        const memoryUsage = process.memoryUsage();
        const totalTime = end - start;
        console.log(`Join completed, total time: ${totalTime}ms`)
        console.log('Total memory usage:', memoryUsage);
        console.log(`Average time: ${totalTime / genres.length}ms`)
        console.log('Average memory usage:', memoryUsage.rss / genres.length);
    } catch (error) {
        console.error('Error performing Sort-Merge join:', error);
    }
}

// Execute the Sort-Merge join function
performSortMergeJoin();
