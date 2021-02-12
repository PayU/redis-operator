const Redis = require("ioredis");

const CLUSTER_ADDRESS = [
    {
        port: Number(process.env.REDIS_PORT),
        host: process.env.REDIS_HOST
    }
]

console.log(CLUSTER_ADDRESS);

const CLUSTER_OPTIONS = {
    scaleReads: "all"
};

const NUMBER_OF_KEYS = Number(process.env.NUMBER_OF_KEYS);

const cluster = new Redis.Cluster(CLUSTER_ADDRESS, CLUSTER_OPTIONS);

async function onClusterReady() {
    console.log('redis cluster connected. populating random data');
    const start = Date.now();

    for (let i = 0; i < NUMBER_OF_KEYS; i++) {
        await cluster.set(`key${i}`, "value");
    }

    const millis = Date.now() - start;
    console.log(`data population finished. total time: ${Math.floor(millis / 1000)} seconds`);
    await cluster.quit();
    process.exitCode = 0;
}

cluster.on('ready', onClusterReady);
cluster.on('error', (err) => {
    console.log(err);
    process.exit(1);
});

