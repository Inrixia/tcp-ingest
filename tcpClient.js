const net = require('net');


const output = new net.Socket();
output.setNoDelay(true)
output.on('close', () => console.log('[output]', 'Connection closed'))
output.on('data', data => console.log(JSON.parse(data.toString())))
output.connect(3000, 'localhost', () => console.log('[output]', 'Connected'));