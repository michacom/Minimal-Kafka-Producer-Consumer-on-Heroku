'use strict';

if ([ 'KAFKA_TOPIC', 'KAFKA_CLIENT_CERT', 'KAFKA_CLIENT_CERT_KEY', 'KAFKA_URL', ].some( (key) => !(key in process.env) ))
	throw new Error(`Missing enviroment value!`);
const { KAFKA_TOPIC, KAFKA_CLIENT_CERT, KAFKA_CLIENT_CERT_KEY, KAFKA_URL, PORT = 5000 } = process.env;

const kafka = require('no-kafka');
const express = require('express');

const fs = require('fs' );
fs.writeFileSync('./client.crt', KAFKA_CLIENT_CERT);
fs.writeFileSync('./client.key', KAFKA_CLIENT_CERT_KEY);

const producer = new kafka.Producer({
	    clientId: 'sample-module-producer',
	    connectionString: KAFKA_URL.replace(/\+ssl/g, ''),
	    ssl: { certFile: './client.crt', keyFile: './client.key', },
	});

producer.init();

const send = msg => producer.send({
		topic: KAFKA_TOPIC,
		partition: 0,
		message: {
		    value: JSON.stringify({
    			sourceSystemCode: 'loudcloud',
    			sourceSystemID: msg.Id,
    		}),
		},
	})

const app = express();

app.
	set('port', PORT).

	use(require('body-parser').json()).

	get('/', ((req, res) => res.send(
		'REST Endpoints:\n' +
		'/module/loudcloud\n'
	))).

	post('/module/loudcloud', ((req, res) => {
		if (Array.isArray(req.body)) {
			for (var m of req.body)
				send(m);
		} else
			send(req.body);

		res.sendStatus(200);
	})).
	
	listen(PORT, (() => console.log.bind(`Node app is running on port ${ PORT }`)));
