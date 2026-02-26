#!/bin/bash

echo "Attente du port de MongoDB..."
# D'abord attendre que le port 27017 du conteneur mongo soit ouvert
until nc -z -v -w30 172.35.0.8 27017; do
  echo "Port MongoDB pas encore ouvert, attente..."
  sleep 2
done

echo "Port ouvert. Attente de l'initialisation complète de MongoDB..."

# Ensuite attendre que le user root soit utilisable
until mongosh --host 172.35.0.8 -u root -p a1B2c3D4e5 --authenticationDatabase admin --eval "db.adminCommand('ping')" >/dev/null 2>&1; do
  echo "MongoDB pas encore prêt, attente..."
  sleep 2
done

echo "MongoDB prêt, lancement de mongorestore..."

mongorestore \
  --host 172.35.0.8 \
  -u root -p a1B2c3D4e5 \
  --authenticationDatabase admin \
  --db backup \
  --collection weather_informations \
  /dump/backup/weather_informations.bson

echo "Données importées avec succès."
tail -f /dev/null
