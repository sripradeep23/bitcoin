# bitcoin

This repro includes
1. The ProducerBitcoin class which streams Bitcoin transaction into kafka from the wss bitcoin API 
2. The Bitcoinconsumer module consumes the data from the kafka and stores in redis.
2. Rest-API's using python framework to access the data from redis.
