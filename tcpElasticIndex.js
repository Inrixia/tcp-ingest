const net = require('net');

const destinationElasticIndex = 'kordia_ais'

let objectsReceived = 0;
let receivedSize = 0;

let dataBuffer = ""
const output = new net.Socket();

const indexQueue = []

const { elastic, object } = require('@inrixia/helpers')

const elasticClient = new (require('@elastic/elasticsearch')).Client({
	"requestTimeout": 1200000,
	"node": "http://127.0.0.1:9200"
});

elastic.createIndex(elasticClient, { 
	index: destinationElasticIndex,
	body: {
		mappings: { "properties": mappings },
		settings: {
			"number_of_shards": 1,
			"number_of_replicas": 0,
			"index": {
				"sort.field": "obsSourceReceiptTimestamp",
				"sort.order": "desc"
			}
		}
	}
}, { logProgress: true, destoryIfExists: true }).catch(err => {
	console.log(err)
	process.exit(1)
})

const indexRecurse = async () => {
	if (indexQueue.length > 0) {
		const coll = {}
		while (indexQueue.length > 0) {
			const item = indexQueue.pop()
			if (coll[item.index] === undefined) coll[item.index] = []
			coll[item.index].push(item.body)
		}
		for (i in coll) {
			for (chunk of object.chunkArray(coll[i], 100000)) {
				await elastic.bulkIndex(elasticClient, chunk, i)
				.catch(err => thread.emit('err', err))
				.then(resp => resp.body.errors && console.log(resp.body.items.map(item => item.index.error)))
			}
		}
	}
	setTimeout(indexRecurse);
}

output.on('close', () => console.log('[output]', 'Connection closed'))
output.on('data', data => {
	receivedSize += data.byteLength
	dataBuffer += data.toString();
	let startIndex = 0;
	while ((messageEndIndex = dataBuffer.indexOf('\n', startIndex)) !== -1) {
		const parsed = JSON.parse(dataBuffer.slice(startIndex, messageEndIndex))
		for (const doc of parsed) {
			doc.obsSourceReceiptTimestamp = date.obsSourceReceiptTimestamp*1000
			doc.obsLocalReceiptTimestamp = date.obsLocalReceiptTimestamp*1000
			if (doc.lat && doc.lon) doc.position = { lat: doc.lat, lon: doc.lon }
			indexQueue.push({
				index: destinationElasticIndex,
				body: doc
			})
		}
		objectsReceived += parsed.length
		process.stdout.write(`Received: ${objectsReceived}, ${(receivedSize/1000/1000).toFixed(2)} MB\r`)
		startIndex = messageEndIndex + 1;
	}
	if (startIndex !== 0) dataBuffer = dataBuffer.slice(startIndex);
})

output.connect(3000, '127.0.0.1', () => console.log('[output]', 'Connected'));
indexRecurse().catch(console.log);


const mappings = {
		"position": { "type": "geo_point" },
        "accuracy": {
          "type": "boolean"
        },
        "ais_version": {
          "type": "long"
        },
        "assigned": {
          "type": "boolean"
        },
        "band": {
          "type": "boolean"
        },
        "callsign": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "course": {
          "type": "float"
        },
        "cs": {
          "type": "boolean"
        },
        "day": {
          "type": "long"
        },
        "destination": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "display": {
          "type": "boolean"
        },
        "draught": {
          "type": "long"
        },
        "dsc": {
          "type": "boolean"
        },
        "dte": {
          "type": "boolean"
        },
        "epfd": {
          "type": "long"
        },
        "gnss": {
          "type": "boolean"
        },
        "heading": {
          "type": "long"
        },
        "hour": {
          "type": "long"
        },
        "imo": {
          "type": "long"
        },
        "lat": {
          "type": "float"
        },
        "lon": {
          "type": "float"
        },
        "maneuver": {
          "type": "long"
        },
        "minute": {
          "type": "long"
        },
        "mmsi": {
          "type": "long"
        },
        "month": {
          "type": "long"
        },
        "msg": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "msg22": {
          "type": "boolean"
        },
        "obsLocalReceiptTimestamp": { "type": "date" },
        "obsSource": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "obsSourceReceiptTimestamp": { "type": "date" },
        "radio": {
          "type": "long"
        },
        "raim": {
          "type": "boolean"
        },
        "regional": {
          "type": "long"
        },
        "repeat": {
          "type": "long"
        },
        "second": {
          "type": "long"
        },
        "shipname": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "shiptype": {
          "type": "long"
        },
        "speed": {
          "type": "float"
        },
        "status": {
          "type": "long"
        },
        "to_bow": {
          "type": "long"
        },
        "to_port": {
          "type": "long"
        },
        "to_starboard": {
          "type": "long"
        },
        "to_stern": {
          "type": "long"
        },
        "turn": {
          "type": "long"
        },
        "type": {
          "type": "long"
        }
      }