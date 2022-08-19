const express = require("express")
const kafka = require("kafka-node")
const mongoose = require("mongoose")
const app = express()
app.use(express.json())


const dbsRunning = async () => {
    mongoose.connect(process.env.MONGO_URL)
    const User = new mongoose.model('user', {
        name: String,
        email: String,
        password: String
    })

    const client = new kafka.KafkaClient({kafkaHost:process.env.KAFKA_BOOSTRAP_SERVERS})
    const consumer = new kafka.Consumer(client, [{topic: process.env.KAFKA_TOPIC}], {
        autoCommit: false
    })

    consumer.on('message', async(message)=>{
        const user = await new User(JSON.parse(message.value))
        await user.save()
    })

    consumer.on("error", (err) => {
        console.log(err)
    })

}

setTimeout(dbsRunning, 1000)

app.listen(process.env.PORT)