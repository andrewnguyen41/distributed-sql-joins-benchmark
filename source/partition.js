const mysql = require('mysql2/promise');
const { fetchMovieByGenre, fetchRatingsForMovies } = require('./db-utils');

async function performPartitionedJoin() {
    try {
        const genres = ["Drama", "Comedy", "Documentary"]
        const start = Date.now();
        for (let genre of genres) {
            const numPartitions = 10; // Adjust the number of partitions as needed

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

            // Initialize partitions
            const moviePartitions = Array.from({ length: numPartitions }, () => []);
            const ratingPartitions = Array.from({ length: numPartitions }, () => []);

            // Assign movies and ratings to partitions based on the join key
            movies.forEach(movie => {
                const partitionIndex = movie.movieId % numPartitions;
                moviePartitions[partitionIndex].push(movie);
            });

            ratings.forEach(rating => {
                const partitionIndex = rating.movieId % numPartitions;
                ratingPartitions[partitionIndex].push(rating);
            });

            // Perform the Partitioned Join
            const joinResult = [];
            for (let partitionIndex = 0; partitionIndex < numPartitions; partitionIndex++) {
                const partitionMovies = moviePartitions[partitionIndex];
                const partitionRatings = ratingPartitions[partitionIndex];

                for (let movie of partitionMovies) {
                    const movieRatings = partitionRatings.filter(rating => rating.movieId === movie.movieId);

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
        console.error('Error performing Partitioned join:', error);
    }
}

// Execute the Partitioned join function
performPartitionedJoin();
