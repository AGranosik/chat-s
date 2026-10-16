present service  scalability
read more about that
logs - > who and when connected
chat service or rather kafka scalability

offline messaging

multiple devices

groups?? mb try to implement this

self-discovery there is added manually
ws connection with client id in query, not safe but its just for learning purposes

no outbox it will be send to kafka and message service will consume that

unit test for training scenario mostly and harder scenario training like wss (not eveything can be mocked)


no retry on rabbitmq or requeue because in ideal scenario whenever the mssages is lost on client side (he got messages 1, 2, 3 (not 4) then number 5, he will asks for missed messages