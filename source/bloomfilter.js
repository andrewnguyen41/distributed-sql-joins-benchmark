const mysql = require('mysql2/promise');
const { BloomFilter } = require('bloom-filters');
const { fetchMovieByGenre, fetchRatingsForMovies } = require('./db-utils');

async function performBloomFilterJoin() {
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

            const ratingFilter = new BloomFilter(1000000, 4); // Bloom filter for ratings
            ratings.forEach(rating => ratingFilter.add(rating.movieId.toString()));

            const joinResult = []
            for (let movie of movies) {
                if (ratingFilter.has(movie.movieId.toString())) {
                    joinResult.push({
                        movieId: movie.movieId,
                        title: movie.title,
                        genres: movie.genres,
                        ratings: ratings.find(r => r.movieId === movie.movieId).content,
                    });
                }
            }
        }
        const end = Date.now();
        const memoryUsage = process.memoryUsage();
        const totalTime = end - start;
        console.log(`Join completed, total time: ${totalTime}ms`)
        console.log('Total memory usage:', memoryUsage);
        console.log(`Average time: ${totalTime/genres.length}ms`)
        console.log('Average memory usage:', memoryUsage.rss/genres.length);
    } catch (error) {
        console.error('Error performing join:', error);
    }
}

performBloomFilterJoin();