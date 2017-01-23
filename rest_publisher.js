'use strict';

if ([ 'KAFKA_TOPIC', 'KAFKA_CLIENT_CERT', 'KAFKA_CLIENT_CERT_KEY', 'KAFKA_URL', ].some( (key) => !(key in process.env) ))
	throw new Error(`Missing ${ key } enviroment value!`);
const { KAFKA_TOPIC, KAFKA_CLIENT_CERT, KAFKA_CLIENT_CERT_KEY, KAFKA_URL, PORT = 5000 } = process.env;

const kafka = require('no-kafka');
const express = require('express');
// const stream = require('stream').Duplex;

const fs = require('fs' );
fs.writeFileSync('./client.crt', KAFKA_CLIENT_CERT);
fs.writeFileSync('./client.key', KAFKA_CLIENT_CERT_KEY);

const producer = new kafka.Producer({
	    clientId: 'sample-module-producer',
	    connectionString: KAFKA_URL.replace(/\+ssl/g, ''),
	    ssl: { certFile: './client.crt', keyFile: './client.key', },
	});

producer.
	init();
	// then(() => stream.on('data', ((msg) =>
	// 	producer.send({
	// 		topic: KAFKA_TOPIC,
	// 		partition: 0,
	// 		message: {
	// 		    value: msg,
	// 		},
	// 	})
	// )));

const send = msg => producer.send({
		topic: KAFKA_TOPIC,
		partition: 0,
		message: {
		    value: msg,
		},
	})

const app = express();

app.
	set('port', PORT).

	get('/', ((req, res) => res.sendStatus(200))).

	post('/module', ((req, res) => {
		var jreq = JSON.parse(req.body);

		if (Array.isArray(jreq)) {
			for (var m of jreq)
				send(m);
				// stream.write(m);
		} else
			send(jreq);
			// stream.write(jreq);

		res.sendStatus(200);
	})).
	
	listen(PORT, (() => console.log.bind(`Node app is running on port ${ PORT }`)));
