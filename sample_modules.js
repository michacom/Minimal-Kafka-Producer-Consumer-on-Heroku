'use strict';

const {
	KAFKA_TOPIC: consumerTopic,
	KAFKA_CLIENT_CERT: cert,
	KAFKA_CLIENT_CERT_KEY: key,
	KAFKA_URL: url,
} = process.env;

const K = require('no-kafka');

const fs = require('fs' );
fs.writeFileSync('./client.crt', process.env.KAFKA_CLIENT_CERT);
fs.writeFileSync('./client.key', process.env.KAFKA_CLIENT_CERT_KEY);

const producer = new Kafka.Producer({
	    clientId: 'sample-module-producer',
	    connectionString: url.replace(/\+ssl/g, ''),
	    ssl: { cert: cert, key: key, },
	});

producer.
	init().
	then(() => {
		for (var msg of sampleGen())
			producer.send({
				topic: KAFKA_TOPIC,
				message: {
				    value: JSON.stringify(msg),
				},
			});
	});

//TODO in production replace the generator with real http calls
function* sampleGen(i = 5) {
	while (--i >= 0)
		yield ({
			sourceSystemCode: 'loudcloud',
			sourceSystemID: randomStringByTemplate('a960f8b0-12ae-4804-b11d-cc8ebd3d60eb'),
		});
}

const CHARS_TEMPLPATE = 'qwertyuiopasdfghjklzxcvbnm1234567890';
const CHARS_TEMPLPATE_LENGTH = CHARS_TEMPLPATE.length;
const randomStringByTemplate = (tmpl) => tmpl.replace(/\w/g, () => CHARS_TEMPLPATE.charAt(Math.random() * CHARS_TEMPLPATE_LENGTH));
