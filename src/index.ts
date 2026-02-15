import net from 'net'

const main = async () => {
    const socket = net.createConnection("127.0.0.1:5673")


    socket.on('readable', () => {
        let data;

        while ((data = socket.read()) !== null) {
            console.log(data)
        }
    })

}

main()