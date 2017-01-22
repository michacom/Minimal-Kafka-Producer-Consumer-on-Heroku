'use strict';

if ([ 'KAFKA_TOPIC', 'KAFKA_CLIENT_CERT', 'KAFKA_CLIENT_CERT_KEY', 'KAFKA_URL', ].some( (key) => !(key in process.env) ))
	throw new Error(`Missing ${ key } enviroment value!`);
const { KAFKA_TOPIC, KAFKA_CLIENT_CERT, KAFKA_CLIENT_CERT_KEY, KAFKA_URL, PORT } = process.env;

const kafka = require('no-kafka');
const express = require('express');
const stream = require('stream').Duplex;

const fs = require('fs' );
fs.writeFileSync('./client.crt', KAFKA_CLIENT_CERT);
fs.writeFileSync('./client.key', KAFKA_CLIENT_CERT_KEY);

const producer = new kafka.Producer({
	    clientId: 'sample-module-producer',
	    connectionString: KAFKA_URL.replace(/\+ssl/g, ''),
	    ssl: { certFile: './client.crt', keyFile: './client.key', },
	});

producer.
	init().
	then(() => stream.on('data', ((msg) =>
		producer.send({
			topic: KAFKA_TOPIC,
			partition: 0,
			message: {
			    value: msg,
			},
		})
	)); );

var reqGen = function* () {};

const app = express();

app.
	set('port', (PORT || 5000)).

	post('/module', ((req, res) => {
		var jreq = JSON.parse(req.body);

		if (Array.isArray(jreq)) {
			for (var m of jreq)
				stream.write(m);
		} else
			stream.write(jreq);

	})).
	
	.listen(app.get('port'), (() => console.log.bind(`Node app is running on port ${ app.get('port') }`)));