const express = require('express');
const mysql = require('mysql2');

const app = express();
const port = 3000;

// Connection configurations for movies_db
const moviesDbConfig = {
  host: 'localhost',
  user: 'root',
  password: 'root',
  database: 'movie_db',
  port: 3306
};

// Connection configurations for ratings_db
const ratingsDbConfig = {
  host: 'localhost',
  user: 'root',
  password: 'root',
  database: 'rating_db',
  port: 3307, // Assuming you have mapped this port for the ratings database
};

// Create MySQL connections
const moviesDbConnection = mysql.createConnection(moviesDbConfig);
const ratingsDbConnection = mysql.createConnection(ratingsDbConfig);

// Connect to databases
moviesDbConnection.connect((err) => {
  if (err) {
    console.error('Error connecting to movies_db:', err.message);
  } else {
    console.log('Connected to movies_db');
  }
});

ratingsDbConnection.connect((err) => {
  if (err) {
    console.error('Error connecting to ratings_db:', err.message);
  } else {
    console.log('Connected to ratings_db');
  }
});

// Define your routes and logic here

// Example route to fetch movies
app.get('/movies', (req, res) => {
  moviesDbConnection.query('SELECT * FROM movie LIMIT 20', (error, results, fields) => {
    if (error) {
      console.error('Error querying movies_db:', error.message);
      res.status(500).send('Internal Server Error');
    } else {
      res.json(results);
    }
  });
});

// Example route to fetch ratings
app.get('/ratings', (req, res) => {
  ratingsDbConnection.query('SELECT * FROM ratings LIMIT 20', (error, results, fields) => {
    if (error) {
      console.error('Error querying ratings_db:', error.message);
      res.status(500).send('Internal Server Error');
    } else {
      res.json(results);
    }
  });
});

// Close database connections when the server is stopped
process.on('SIGINT', () => {
  moviesDbConnection.end();
  ratingsDbConnection.end();
  process.exit();
});

// Start the server
app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});
