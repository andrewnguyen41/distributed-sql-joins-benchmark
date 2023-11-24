const mysql = require('mysql2/promise');

// Create connections to the databases
const ratingDB = mysql.createPool({
    host: "localhost",
    user: "root",
    password: "root",
    database: "rating_db",
    port: 3307
});

const movieDB = mysql.createPool({
    host: "localhost",
    user: "root",
    password: "root",
    database: "movie_db",
    port: 3306
});

async function fetchMovieByGenre(genre) {
    const connection = await movieDB.getConnection();
    try {
        const [rows] = await connection.query('SELECT * FROM movie WHERE genres = ?', [genre]);
        return rows; // Return the first row (assuming movieId is unique)
    } catch (error) {
        throw error;
    } finally {
        connection.release();
    }
}

// Function to fetch ratings for a specific movie ID
async function fetchRatingsForMovies(movieIds) {
    const connection = await ratingDB.getConnection();
    try {
        const [rows] = await connection.query('SELECT * FROM rating WHERE movieId IN (?)', [movieIds]);
        return rows; // Return the first row (assuming movieId is unique)
    } catch (error) {
        throw error;
    } finally {
        connection.release();
    }
}

module.exports = {
    fetchMovieByGenre,
    fetchRatingsForMovies
};
