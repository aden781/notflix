const express = require("express")
const mongodb = require("mongodb")
const bodyParser = require('body-parser')
const amqp = require("amqplib")

const RABBIT = process.env.RABBIT

function connectedRabbit() {
    return amqp.connect(RABBIT)
        .then(messagingConnection => {
            return messagingConnection.createChannel()
        })
}


if (!process.env.DBHOST) {
    throw new Error("Please specify the databse host using environment variable DBHOST.")
}

if (!process.env.DBNAME) {
    throw new Error("Please specify the name of the database using environment variable DBNAME")
}

const DBHOST = process.env.DBHOST
const DBNAME = process.env.DBNAME

//
// Connect to the database.
//
function connectDb() {
    return mongodb.MongoClient.connect(DBHOST)
        .then(client => {
            return client.db(DBNAME)
        })
}

//
// Setup event handlers.
//
function setupHandlers(app, db, messageChannel) {

    const videosCollection = db.collection("videos")


    function consumeViewedMessage(msg) {
        console.log("Received a 'viewed' message")

        const parsedMsg = JSON.parse(msg.content.toString()) 

        return videosCollection.insertOne({ videoPath: parsedMsg.videoPath })
            .then(() => {
                console.log("Acknowledging message was handled.")

                messageChannel.ack(msg) 
            })
    }

    return messageChannel.assertExchange("viewed", "fanout")
        .then(() => {

            return messageChannel.assertQueue("", {exclusive: true})
        })
        .then(response => {
            const queueName = response.queueName
            return messageChannel.bindQueue(queueName, "viewed", "")
                .then(() => {
                    return messageChannel.consume(queueName, consumeViewedMessage)
                })
        })
}

    // app.post("/viewed", (req, res) => { // Handle the "viewed" message via HTTP POST request.
    //     const videoPath = req.body.videoPath // Read JSON body from HTTP request.
    //     videosCollection.insertOne({ videoPath: videoPath }) // Record the "view" in the database.
    //         .then(() => {
    //             console.log(`Added video ${videoPath} to history.`)
    //             res.sendStatus(200)
    //         })
    //         .catch(err => {
    //             console.error(`Error adding video ${videoPath} to history.`)
    //             console.error(err && err.stack || err)
    //             res.sendStatus(500)
    //         })
    // })

    // app.get("/history", (req, res) => {
    //     const skip = parseInt(req.query.skip)
    //     const limit = parseInt(req.query.limit)
    //     videosCollection.find()
    //         .skip(skip)
    //         .limit(limit)
    //         .toArray()
    //         .then(documents => {
    //             res.json({ history: documents })
    //         })
    //         .catch(err => {
    //             console.error(`Error retrieving history from database.`)
    //             console.error(err && err.stack || err)
    //             res.sendStatus(500)
    //         })
    // })

// }

//
// Start the HTTP server.
//
function startHttpServer(db, messageChannel) {
    return new Promise(resolve => { // Wrap in a promise so we can be notified when the server has started.
        const app = express()
        app.use(bodyParser.json()) // Enable JSON body for HTTP requests.
        setupHandlers(app, db, messageChannel)

        const port = process.env.PORT && parseInt(process.env.PORT) || 3000
        app.listen(port, () => {
            resolve() // HTTP server is listening, resolve the promise.
        })
    })
}

//
// Application entry point.
//
function main() {
    return connectDb(DBHOST)            // Connect to the database...
        .then(db => {
            return connectedRabbit()
                .then(messageChannel => {
                    return startHttpServer(db, messageChannel)
                })
            // return startHttpServer(db) // start the HTTP server.
        })
}

main()
    .then(() => console.log("Microservice online."))
    .catch(err => {
        console.error("Microservice failed to start.")
        console.error(err && err.stack || err)
    })