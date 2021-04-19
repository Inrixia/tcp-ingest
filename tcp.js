const net = require('net');

let objectsReceived = 0;
let receivedSize = 0;

let dataBuffer = ""
const output = new net.Socket();
output.on('close', () => console.log('[output]', 'Connection closed'))
output.on('data', data => {
	receivedSize += data.byteLength
	dataBuffer += data.toString();
	let startIndex = 0;
	while ((messageEndIndex = dataBuffer.indexOf('\n', startIndex)) !== -1) {
		const parsed = JSON.parse(dataBuffer.slice(startIndex, messageEndIndex))
		console.log(parsed)
		objectsReceived += parsed.length
		// process.stdout.write(`Received: ${objectsReceived}, ${(receivedSize/1000/1000).toFixed(2)} MB\r`)
		startIndex = messageEndIndex + 1;
	}
	if (startIndex !== 0) dataBuffer = dataBuffer.slice(startIndex);
})
output.connect(3000, '', () => console.log('[output]', 'Connected'));