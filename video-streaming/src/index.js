const http = require("http")
const express = require("express")
const amqp = require('amqplib')
const fs = require("fs")

const RABBIT = process.env.RABBIT

function connectRabbit() {

    console.log(`Connecting to RabbitMQ server at ${RABBIT}.`)

    return amqp.connect(RABBIT) // Connect to the RabbitMQ server.
        .then(connection => {
            console.log("Connected to RabbitMQ.")

            return connection.createChannel()
                .then(messageChannel => {
                    return messageChannel.assertExchange("viewed", "fanout")
                    .then(() => {
                        return messageChannel
                    })
                })

        })
    }


    function sendViewedMessage(messageChannel, videoPath) {
        console.log(`Publishing message on "viewed" queue.`)
    
        const msg = { videoPath: videoPath }
        const jsonMsg = JSON.stringify(msg)
        messageChannel.publish("viewed", "", Buffer.from(jsonMsg)) 
    }

    //
    // Setup event handlers.
    //
    function setupHandlers(app, messageChannel) {
        app.get("/video", (req, res) => { // Route for streaming video.

            const videoPath = "./videos/SampleVideo_1280x720_1mb.mp4"
            fs.stat(videoPath, (err, stats) => {
                if (err) {
                    console.error("An error occurred ")
                    res.sendStatus(500)
                    return
                }

                res.writeHead(200, {
                    "Content-Length": stats.size,
                    "Content-Type": "video/mp4",
                })

                fs.createReadStream(videoPath).pipe(res)
                sendViewedMessage(messageChannel, videoPath)
            })
        })
    }

    //
    // Start the HTTP server.
    //
    function startHttpServer(messageChannel) {
        return new Promise((resolve, reject) => { // Wrap in a promise so we can be notified when the server has started.
            const app = express()
            setupHandlers(app, messageChannel)

            const port = process.env.PORT && parseInt(process.env.PORT) || 3000
            app.listen(port, () => {
                resolve()
            })
        })
    }

    //
    // Application entry point.
    //
    function main() {
        return connectRabbit()                          // Connect to RabbitMQ...
            .then(messageChannel => {                   // then...
                return startHttpServer(messageChannel) // start the HTTP server.
            })
    }

    main()
        .then(() => console.log("Microservice are online."))
        .catch(err => {
            console.error("Microservice failed to start.")
            console.error(err && err.stack || err)
        })