const express = require('express');
const mysql = require('mysql');
const fetch = require('node-fetch');
const Redis = require("ioredis");
const Redlock = require('redlock');
const redisClient = new Redis();
const redlock = new Redlock([ redisClient ]);

// Create a MySQL connection pool
const pool = mysql.createPool({
    connectionLimit : 10,
    host : 'localhost',
    user : 'rex',
    password : '12345678',
    database : 'filerepo'
});

const app = express();

app.use(express.json());

app.use(express.urlencoded({extended : true}));

app.get('/api/set-data', async (req, res) => {
    const lockKey = 'lock-key';
    const title = req.body.title;
    const message = req.body.message;

    redlock.lock(lockKey, 1000)
        .then((lock) => {
            console.log('Lock acquired:', lock.resource);
            return redisClient.get(title);
        })
        .then(async (value) => {
            console.log('Value:', value);
            if (value == null) {
                const query = `
                INSERT INTO messagetable (title, message)
                VALUES (?, ?)
                ON DUPLICATE KEY UPDATE message = ?;
              `;
                const values = [ title, message, message ];
                await executeQuery(query, values);
                console.log('Value inserted into MySQL');
                await redisClient.set(title, message, "EX", 60);
                res.status(200).send(message);
            } else {
                res.status(200).send(value);
            }
        })
        .then(() => { console.log('Lock released'); })
        .catch((error) => {
            console.error('Error acquiring or releasing lock:', error);
        });
});

// Function to execute SQL queries
function executeQuery(query, values) {
    return new Promise((resolve, reject) => {
        pool.getConnection((err, connection) => {
            if (err) {
                reject(err);
                return;
            }

            connection.query(query, values, (error, results) => {
                connection.release();

                if (error) {
                    reject(error);
                    return;
                }

                resolve(results);
            });
        });
    });
}

// Start the server
app.listen(3000, () => { console.log('Server listening on port 3000'); });
