const express = require('express');
const mysql = require('mysql');
const fetch = require('node-fetch');
const Redis = require("ioredis");
const Redlock = require('redlock');
const redisClient = new Redis();

var redlock = new Redlock(
    // you should have one client for each independent redis node
    // or cluster
    [ redisClient ], {
        // the expected clock drift; for more details
        // see http://redis.io/topics/distlock
        driftFactor : 0.01, // multiplied by lock ttl to determine drift time

        // the max number of times Redlock will attempt
        // to lock a resource before erroring
        retryCount : 700,

        // the time in ms between attempts
        retryDelay : 400, // time in ms

        // the max time in ms randomly added to retries
        // to improve performance under high contention
        // see https://www.awsarchitectureblog.com/2015/03/backoff.html
        retryJitter : 200, // time in ms
    });

// Create a MySQL connection pool
const pool = mysql.createPool({
    connectionLimit : 10,
    host : 'localhost',
    user : 'rex',
    password : '12345678',
    database : 'filerepo'
});

const app = express();

let count = 0

app.use(express.json());

app.use(express.urlencoded({extended : true}));

app.get('/api/set-data', async (req, res) => {
    const lockKey = 'locks:lock-key';
    const title = req.query.title;
    const message = req.query.message;

    console.log('Count:', count);
    count = count + 1;

    redlock.lock(lockKey, 2200)
        .then((lock) => {
            console.log('Lock acquired:', lock.resource);
            return redisClient.get(title);
        })
        .then(async (value) => {
            if (value == null) {
                console.log('Value is not found in Redis');
                const query = `
                INSERT INTO messagetable (title, message)
                VALUES (?, ?)
                ON DUPLICATE KEY UPDATE message = ?;
              `;
                const values = [ title, message, message ];
                await executeQuery(query, values);

                console.log('Start mock fetch....');
                await fetchApiData();

                console.log('Value inserted into MySQL');
                await redisClient.set(title, message, "EX", 5);
                res.status(200).send(message);
            } else {
                console.log('Value is found in Redis: ', value);
                res.status(200).send(value);
            }
        })
        .then(() => { console.log('Lock released'); })
        .catch((error) => {
            if (error.name === 'LockError') {
                console.error('Error acquiring lock:', error.message);
                res.status(500).send('Failed to acquire lock');
            } else {
                console.error('Error acquiring or releasing lock:', error);
                res.status(500).send('Internal server error');
            }
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

async function fetchApiData() {
    try {
        const response = await fetch('https://api.rapidmock.com/mocks/f6GeB');
        const data = await response.json();
        return data;
    } catch (error) {
        console.error('Error fetching API data:', error);
        throw error;
    }
}

// Start the server
app.listen(3000, () => { console.log('Server listening on port 3000'); });
