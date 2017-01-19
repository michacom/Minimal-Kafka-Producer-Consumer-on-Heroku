'use strict';

if ([ 'KAFKA_TOPIC', 'KAFKA_CLIENT_CERT', 'KAFKA_CLIENT_CERT_KEY', 'KAFKA_URL', ].some( (key) => !(key in process.env) ))
	throw new Error(`Missing ${ key } enviroment value!`);
const { KAFKA_TOPIC, KAFKA_CLIENT_CERT, KAFKA_CLIENT_CERT_KEY, KAFKA_URL } = process.env;

const K = require('no-kafka');

const producer = new K.Producer({
	    clientId: 'sample-module-producer',
	    connectionString: KAFKA_URL.replace(/\+ssl/g, ''),
	    ssl: { cert: KAFKA_CLIENT_CERT, key: KAFKA_CLIENT_CERT_KEY, },
	});

console.log('producer', producer);
throw new Error('STOP THERE');

producer.
	init().
	then(() => {
		for (var msg of sampleGen(1))
			producer.send({
				topic: KAFKA_TOPIC,
				partition: 0,
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
