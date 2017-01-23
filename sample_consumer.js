'use strict';

if ([ 'KAFKA_TOPIC', 'KAFKA_CLIENT_CERT', 'KAFKA_CLIENT_CERT_KEY', 'KAFKA_URL', ].some( (key) => !(key in process.env) ))
	throw new Error(`Missing enviroment value!`);
const { KAFKA_TOPIC, KAFKA_CLIENT_CERT, KAFKA_CLIENT_CERT_KEY, KAFKA_URL } = process.env;

const K = require('no-kafka');
;
const fs = require('fs' );
fs.writeFileSync('./client.crt', KAFKA_CLIENT_CERT);
fs.writeFileSync('./client.key', KAFKA_CLIENT_CERT_KEY);

const consumer = new K.SimpleConsumer({
    idleTimeout: 100,
    clientId: 'sample-module-consumer',
    connectionString: KAFKA_URL.replace(/\+ssl/g,''),
    ssl: { certFile: './client.crt', keyFile: './client.key', }
});

return consumer.
	init().
	then(() => {
		console.log(`subscribed to KAFKA topic: ${ KAFKA_TOPIC }`);

		consumer.subscribe(KAFKA_TOPIC, arr => {
			for ({ message: { value } } of arr)
				console.log(`from KAFKA topic: ${ value.toString() }`)
		})
	});
